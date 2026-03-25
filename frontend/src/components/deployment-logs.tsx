import type { Rivet } from "@rivet-gg/cloud";
import { faTriangleExclamation, Icon } from "@rivet-gg/icons";
import type { Virtualizer } from "@tanstack/react-virtual";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { ErrorDetails } from "@/components/actors";
import { VirtualScrollArea } from "@/components/virtual-scroll-area";
import { AnsiText } from "./lib/ansi";
import { logfmt } from "./lib/logfmt";
import { cn } from "./lib/utils";
import { ScrollArea } from "./ui/scroll-area";
import { Skeleton } from "./ui/skeleton";
import { useDeploymentLogsStream } from "./use-deployment-logs-stream";

const SKELETON_KEYS = [
	"a", "b", "c", "d", "e", "f", "g", "h", "i", "j",
	"k", "l", "m", "n", "o", "p", "q", "r", "s", "t",
	"u", "v", "w", "x", "y", "z", "aa", "ab", "ac", "ad",
	"ae", "af", "ag", "ah", "ai", "aj", "ak", "al", "am", "an",
];

// Columns extracted into dedicated display columns.
const EXTRACTED_KEYS = new Set(["level", "lvl", "msg", "message"]);

// Level column is fixed — badge text is at most 5 chars + padding.
const COL_LEVEL = "w-[7ch] shrink-0";

// Timestamp and region columns are sized dynamically via CSS custom properties
// set on the outer wrapper. Rows and header read them via inline style.
const COL_TIMESTAMP_STYLE = { width: "var(--col-ts)", flexShrink: 0 } as const;
const COL_REGION_STYLE = { width: "var(--col-region)", flexShrink: 0 } as const;

type LevelVariant = "error" | "warn" | "info" | "debug" | "trace" | "default";

function getLevelVariant(level: string): LevelVariant {
	const l = level.toLowerCase();
	if (l === "error" || l === "err" || l === "fatal" || l === "crit") return "error";
	if (l === "warn" || l === "warning") return "warn";
	if (l === "info") return "info";
	if (l === "debug" || l === "dbg") return "debug";
	if (l === "trace") return "trace";
	return "default";
}

const LEVEL_CLASSES: Record<LevelVariant, string> = {
	error: "text-destructive-foreground bg-destructive/40",
	warn: "text-yellow-300 bg-yellow-900/40",
	info: "text-green-300 bg-green-900/30",
	debug: "text-blue-300 bg-blue-900/30",
	trace: "text-neutral-400 bg-neutral-800/50",
	default: "text-neutral-300 bg-neutral-800/50",
};

interface ParsedMessage {
	level: string | null;
	msg: string | null;
	extras: Array<[string, string]>;
	isLogfmt: boolean;
}

function parseMessage(raw: string): ParsedMessage {
	if (!raw.includes("=")) {
		return { level: null, msg: null, extras: [], isLogfmt: false };
	}

	const parsed = logfmt.parse(raw);
	const keys = Object.keys(parsed);
	if (keys.length === 0) {
		return { level: null, msg: null, extras: [], isLogfmt: false };
	}

	const level =
		typeof parsed.level === "string"
			? parsed.level
			: typeof parsed.lvl === "string"
				? parsed.lvl
				: null;

	const msg =
		typeof parsed.msg === "string"
			? parsed.msg
			: typeof parsed.message === "string"
				? parsed.message
				: null;

	const extras: Array<[string, string]> = [];
	for (const key of keys) {
		if (EXTRACTED_KEYS.has(key)) continue;
		const val = parsed[key];
		if (val === null || val === undefined) continue;
		extras.push([key, typeof val === "object" ? JSON.stringify(val) : String(val)]);
	}

	return { level, msg, extras, isLogfmt: true };
}

// Measures the widest timestamp and region seen so far and returns them as
// CSS ch values. Uses a ref so widths only ever grow, never shrink (avoids
// re-layout churn on every new log line).
function useColWidths(logs: Rivet.LogHistoryResponseItem[]) {
	const maxRef = useRef({ ts: 0, region: 0 });

	return useMemo(() => {
		let changed = false;
		for (const log of logs) {
			if (log.timestamp.length > maxRef.current.ts) {
				maxRef.current.ts = log.timestamp.length;
				changed = true;
			}
			// +2 for the surrounding brackets rendered in the UI.
			const rLen = log.region ? log.region.length + 2 : 0;
			if (rLen > maxRef.current.region) {
				maxRef.current.region = rLen;
				changed = true;
			}
		}
		void changed;
		return {
			"--col-ts": `${maxRef.current.ts || 24}ch`,
			"--col-region": `${maxRef.current.region || 12}ch`,
		} as React.CSSProperties;
	}, [logs]);
}

interface DeploymentLogsProps {
	pool: string;
	filter?: string;
	region?: string;
	paused?: boolean;
	logsRef?: React.MutableRefObject<Rivet.LogHistoryResponseItem[]>;
}

interface LogRowData {
	className?: string;
	entry?: Rivet.LogHistoryResponseItem;
	isSentinel?: boolean;
	isLoadingMore?: boolean;
}

function LogRow({ entry, isSentinel, isLoadingMore, ...props }: LogRowData) {
	const parsed = useMemo(() => (entry ? parseMessage(entry.message) : null), [entry]);

	if (isSentinel) {
		return (
			<div
				{...props}
				className={cn(
					"px-4 py-1 border-b text-muted-foreground/50 italic",
					props.className,
				)}
			>
				{isLoadingMore ? "Loading older logs…" : "Scroll to top to load older logs"}
			</div>
		);
	}

	if (!entry || !parsed) return null;

	const isStderr = entry.stream === "stderr";

	return (
		<div
			{...props}
			className={cn(
				"flex gap-3 whitespace-pre-wrap break-words px-4 py-1 border-b text-xs",
				isStderr ? "text-red-400" : "text-muted-foreground",
				props.className,
			)}
		>
			<span className="text-neutral-500 shrink-0" style={COL_TIMESTAMP_STYLE}>
				{entry.timestamp}
			</span>
			<span className="text-neutral-600 shrink-0" style={COL_REGION_STYLE}>
				{entry.region ? `[${entry.region}]` : ""}
			</span>
			<LevelCell level={parsed.isLogfmt ? parsed.level : null} isStderr={isStderr} />
			<span className="flex-1 min-w-0">
				{parsed.isLogfmt ? (
					<>
						{parsed.msg ? <AnsiText text={parsed.msg} /> : null}
						{parsed.extras.length > 0 ? (
							<span className="text-neutral-500 ml-2">
								{parsed.extras.map(([k, v]) => (
									<span key={k} className="mr-2">
										<span className="text-neutral-400">{k}</span>
										<span className="text-neutral-600">=</span>
										<span>{v}</span>
									</span>
								))}
							</span>
						) : null}
					</>
				) : (
					<AnsiText text={entry.message} />
				)}
			</span>
		</div>
	);
}

function LevelCell({ level, isStderr }: { level: string | null; isStderr: boolean }) {
	if (isStderr && !level) {
		return (
			<span className={COL_LEVEL}>
				<span className={cn("px-1 rounded text-[10px] font-semibold uppercase", LEVEL_CLASSES.error)}>
					err
				</span>
			</span>
		);
	}
	if (!level) {
		return <span className={COL_LEVEL} />;
	}
	const variant = getLevelVariant(level);
	return (
		<span className={COL_LEVEL}>
			<span className={cn("px-1 rounded text-[10px] font-semibold uppercase", LEVEL_CLASSES[variant])}>
				{level.slice(0, 5)}
			</span>
		</span>
	);
}

function LogsHeader() {
	return (
		<div className="flex gap-3 px-4 py-1 border-b text-xs font-semibold uppercase tracking-wider text-neutral-500 bg-card shrink-0 select-none">
			<span className="shrink-0" style={COL_TIMESTAMP_STYLE}>Timestamp</span>
			<span className="shrink-0" style={COL_REGION_STYLE}>Region</span>
			<span className={COL_LEVEL}>Level</span>
			<span className="flex-1">Message</span>
		</div>
	);
}

export function DeploymentLogs({
	pool,
	filter,
	region,
	paused,
	logsRef,
}: DeploymentLogsProps) {
	const { logs, isLoading, error, streamError, isLoadingMore, hasMore, loadMoreHistory } =
		useDeploymentLogsStream({ pool, filter, region, paused });

	const viewportRef = useRef<HTMLDivElement>(null);
	const virtualizerRef = useRef<Virtualizer<HTMLDivElement, Element>>(null);
	const [follow, setFollow] = useState(true);
	// Track the log count before a load-more so we can restore scroll position.
	const prevLogCountRef = useRef(0);

	const colWidths = useColWidths(logs);

	// When hasMore, index 0 is the sentinel row; real logs start at index 1.
	const sentinelOffset = hasMore ? 1 : 0;
	const totalCount = logs.length + sentinelOffset;

	useEffect(() => {
		if (follow && !isLoading && virtualizerRef.current && logs.length > 0) {
			// https://github.com/TanStack/virtual/issues/537
			const rafId = requestAnimationFrame(() => {
				virtualizerRef.current?.scrollToIndex(totalCount - 1, {
					align: "end",
				});
			});
			return () => cancelAnimationFrame(rafId);
		}
	}, [totalCount, logs.length, follow, isLoading]);

	// After prepending older history, scroll to restore the previously-first row.
	const wasLoadingMoreRef = useRef(false);
	useEffect(() => {
		if (wasLoadingMoreRef.current && !isLoadingMore && logs.length > prevLogCountRef.current) {
			const addedCount = logs.length - prevLogCountRef.current;
			const rafId = requestAnimationFrame(() => {
				// +1 to skip sentinel row at index 0.
				virtualizerRef.current?.scrollToIndex(addedCount + sentinelOffset, {
					align: "start",
				});
			});
			return () => cancelAnimationFrame(rafId);
		}
		wasLoadingMoreRef.current = isLoadingMore;
	}, [isLoadingMore, logs.length, sentinelOffset]);

	useEffect(() => {
		if (logsRef) {
			logsRef.current = logs;
		}
	}, [logs, logsRef]);

	const handleScrollChange = useCallback(
		(instance: Virtualizer<HTMLDivElement, Element>) => {
			const isAtBottom =
				(instance.range?.endIndex ?? 0) >= totalCount - 1;
			if (isAtBottom) {
				return setFollow(true);
			}
			if (instance.scrollDirection === "backward") {
				setFollow(false);
				// Load more when the sentinel row comes into view.
				if ((instance.range?.startIndex ?? 1) === 0 && hasMore && !isLoadingMore) {
					prevLogCountRef.current = logs.length;
					loadMoreHistory();
				}
			}
		},
		[totalCount, logs.length, hasMore, isLoadingMore, loadMoreHistory],
	);

	if (isLoading) {
		return (
			<div className="h-full flex flex-col ">
				<ScrollArea
					className="w-full h-full"
					viewportProps={{ className: "p-2" }}
				>
					{SKELETON_KEYS.map((key) => (
						<Skeleton
							key={key}
							className="w-full h-6 mb-2 last:mb-0"
						/>
					))}
				</ScrollArea>
			</div>
		);
	}

	if (logs.length === 0) {
		if (error) {
			return (
				<div className="h-full flex-1 flex items-center justify-center">
					<div className="max-w-md flex flex-col items-center justify-center flex-1">
						<Icon
							icon={faTriangleExclamation}
							className="text-red-500 mb-2 text-2xl"
						/>
						<div className="text-center">
							<div className="mb-1">Failed to load logs.</div>
							<ErrorDetails error={error} className="text-sm" />
						</div>
					</div>
				</div>
			);
		}
		return (
			<div className="h-full flex flex-1 flex-col items-center justify-center">
				<p>No logs available.</p>
				<p className="text-muted-foreground text-xs mt-1">
					Logs will appear here as they stream in.
				</p>
			</div>
		);
	}

	return (
		<div
			className="h-full font-mono text-xs text-neutral-100 overflow-hidden flex flex-col"
			style={colWidths}
		>
			{streamError ? (
				<div className="flex items-center gap-2 px-4 py-2 bg-destructive/20 text-destructive-foreground text-xs border-b border-destructive/40 shrink-0">
					<Icon icon={faTriangleExclamation} className="shrink-0" />
					<span>Stream error: {streamError}</span>
				</div>
			) : null}
			<LogsHeader />
			<VirtualScrollArea<LogRowData>
				virtualizerRef={virtualizerRef}
				viewportRef={viewportRef}
				onChange={handleScrollChange}
				count={totalCount}
				estimateSize={() => 24}
				className="w-full flex-1 min-h-0"
				scrollerProps={{
					className: "w-full",
				}}
				viewportProps={{}}
				getRowData={(index) => {
					if (hasMore && index === 0) {
						return { isSentinel: true, isLoadingMore };
					}
					return { entry: logs[index - sentinelOffset] };
				}}
				row={LogRow}
			/>
		</div>
	);
}
