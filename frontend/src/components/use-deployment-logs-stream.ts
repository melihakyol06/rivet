import { type Rivet, RivetSse } from "@rivet-gg/cloud";
import { type InfiniteData, useInfiniteQuery, useQueryClient } from "@tanstack/react-query";
import { useCallback, useEffect, useRef, useState } from "react";
import { useCloudNamespaceDataProvider } from "@/components/actors";
import { clerk } from "@/lib/auth";
import { cloudEnv } from "@/lib/env";

const MAX_RETRIES = 8;
const BASE_DELAY_MS = 1_000;

async function sleep(ms: number, signal: AbortSignal) {
	return new Promise<void>((resolve) => {
		const timeout = setTimeout(resolve, ms);
		signal.addEventListener(
			"abort",
			() => {
				clearTimeout(timeout);
				resolve();
			},
			{ once: true },
		);
	});
}

type RawInfiniteData = InfiniteData<Rivet.LogHistoryResponseItem[]>;

interface UseDeploymentLogsStreamOptions {
	pool: string;
	filter?: string;
	region?: string;
	paused?: boolean;
}

export function useDeploymentLogsStream({
	pool,
	filter,
	region,
	paused = false,
}: UseDeploymentLogsStreamOptions) {
	const dataProvider = useCloudNamespaceDataProvider();
	const queryClient = useQueryClient();

	const queryOpts = dataProvider.currentNamespaceLogsHistoryInfiniteQueryOptions({
		pool,
		contains: filter,
		region,
	});

	const { data, isFetching, isFetchingPreviousPage, hasPreviousPage, fetchPreviousPage, error } =
		useInfiniteQuery(queryOpts);

	const logs = data?.logs ?? [];

	const [streamError, setStreamError] = useState<string | null>(null);

	const pausedRef = useRef(paused);
	useEffect(() => {
		pausedRef.current = paused;
	}, [paused]);

	const pendingRef = useRef<Rivet.LogHistoryResponseItem[]>([]);

	// Keep a ref to the query key so the SSE effect doesn't need it as a dep.
	const queryKeyRef = useRef(queryOpts.queryKey);
	useEffect(() => {
		queryKeyRef.current = queryOpts.queryKey;
	}, [queryOpts.queryKey]);

	const appendToCache = useCallback((items: Rivet.LogHistoryResponseItem[]) => {
		queryClient.setQueryData(queryKeyRef.current, (prev: RawInfiniteData | undefined) => {
			if (!prev) return prev;
			const pages = [...prev.pages];
			pages[pages.length - 1] = [...(pages.at(-1) ?? []), ...items];
			return { ...prev, pages };
		});
	}, [queryClient]);

	// SSE stream — appends live entries directly into the query cache.
	useEffect(() => {
		const controller = new AbortController();

		async function stream() {
			const options = {
				baseUrl: cloudEnv().VITE_APP_CLOUD_API_URL,
				environment: "",
				token: async () => (await clerk.session?.getToken()) || "",
			};

			for (let attempt = 0; attempt <= MAX_RETRIES; attempt++) {
				if (controller.signal.aborted) return;

				try {
					const events = RivetSse.streamLogs(
						options,
						dataProvider.project,
						dataProvider.cloudNamespace,
						pool,
						{
							contains: filter || undefined,
							region: region || undefined,
							abortSignal: controller.signal,
						},
					);

					for await (const event of events) {
						if (controller.signal.aborted) return;

						if (event.event === "error") {
							setStreamError(event.data.message);
							continue;
						}

						if (event.event !== "log") continue;

						setStreamError(null);

						if (pausedRef.current) {
							pendingRef.current.push(event.data);
							continue;
						}

						const toAppend = [...pendingRef.current, event.data];
						pendingRef.current = [];
						appendToCache(toAppend);
					}
				} catch (err) {
					if ((err as Error).name === "AbortError") return;
					console.error(`Log stream error (attempt ${attempt + 1}):`, err);
				}

				if (attempt < MAX_RETRIES) {
					await sleep(BASE_DELAY_MS * 2 ** attempt, controller.signal);
				}
			}
		}

		void stream();
		return () => controller.abort();
	}, [dataProvider.project, dataProvider.cloudNamespace, pool, filter, region, appendToCache]);

	// Flush pending entries when unpaused.
	useEffect(() => {
		if (paused || pendingRef.current.length === 0) return;
		const toAppend = pendingRef.current;
		pendingRef.current = [];
		appendToCache(toAppend);
	}, [paused, appendToCache]);

	return {
		logs,
		isLoading: isFetching && logs.length === 0,
		error: error?.message ?? null,
		streamError,
		isLoadingMore: isFetchingPreviousPage,
		hasMore: hasPreviousPage,
		loadMoreHistory: fetchPreviousPage,
	};
}
