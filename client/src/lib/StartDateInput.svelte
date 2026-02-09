<script lang="ts">
  import { createEventDispatcher, tick } from "svelte";
  import {
    formatStartInputUtc,
    normalizeStartInput,
    parseStartInputUtcMs,
  } from "../../../src/shared/startInput.js";

  interface ChangeDetail {
    value: string;
    ms: number | null;
  }

  type SegmentId = "day" | "month" | "year" | "hour" | "minute";

  interface Segment {
    id: SegmentId;
    start: number;
    end: number;
    placeholder: string;
  }

  const MASK = "--/--/----, --:--";
  const MASK_RE =
    /^(\d{2}|--)\/(\d{2}|--)\/(\d{4}|----), (\d{2}|--):(\d{2}|--)$/;
  const SEGMENTS: readonly Segment[] = [
    { id: "day", start: 0, end: 2, placeholder: "--" },
    { id: "month", start: 3, end: 5, placeholder: "--" },
    { id: "year", start: 6, end: 10, placeholder: "----" },
    { id: "hour", start: 12, end: 14, placeholder: "--" },
    { id: "minute", start: 15, end: 17, placeholder: "--" },
  ];

  export let value = "";
  export let className = "";
  export let placeholder = "DD/MM/YYYY, HH:mm (UTC)";

  const dispatch = createEventDispatcher<{ change: ChangeDetail }>();

  let inputEl: HTMLInputElement | null = null;
  let calendarEl: HTMLInputElement | null = null;
  let draft = toDisplay(value);
  let editing = false;
  let pointerFocus = false;
  let digitBuffer = "";
  let digitBufferSegment: SegmentId | null = null;
  let digitBufferTs = 0;

  $: if (!editing) {
    const next = toDisplay(value);
    if (draft !== next) draft = next;
  }

  function handleInput(event: Event): void {
    draft = (event.target as HTMLInputElement).value;
    resetDigitBuffer();
  }

  function handlePointerDown(): void {
    pointerFocus = true;
  }

  function handleFocus(): void {
    editing = true;
    if (pointerFocus) return;
    selectSegmentSoon("day");
  }

  function handleBlur(): void {
    editing = false;
    pointerFocus = false;
    resetDigitBuffer();
    commit(draft);
  }

  function handleClick(): void {
    pointerFocus = false;
    if (!editing) return;
    selectCurrentSegmentSoon();
  }

  function handlePaste(event: ClipboardEvent): void {
    const pasted = event.clipboardData?.getData("text")?.trim() ?? "";
    if (!pasted) return;
    if (parseStartInputUtcMs(pasted) === null) return;
    event.preventDefault();
    resetDigitBuffer();
    commit(pasted);
    selectSegmentById("day");
  }

  function handleKeydown(event: KeyboardEvent): void {
    if (event.key === "Enter") {
      event.preventDefault();
      inputEl?.blur();
      return;
    }
    if (event.key === "Escape") {
      event.preventDefault();
      resetDigitBuffer();
      draft = toDisplay(value);
      inputEl?.blur();
      return;
    }

    const segment = activeSegment();
    if (!segment) return;

    if (event.key === "ArrowUp" || event.key === "ArrowDown") {
      event.preventDefault();
      adjustSegment(segment.id, event.key === "ArrowUp" ? 1 : -1);
      selectSegmentSoon(segment.id);
      return;
    }

    if (event.key === "Backspace" || event.key === "Delete") {
      event.preventDefault();
      clearSegment(segment.id);
      selectSegmentSoon(segment.id);
      return;
    }

    if (/^\d$/.test(event.key)) {
      event.preventDefault();
      writeDigit(segment.id, event.key);
    }
  }

  function commit(raw: string): void {
    const trimmed = raw.trim();
    if (!trimmed) {
      draft = "";
      emitIfChanged("", null);
      return;
    }
    const ms = parseStartInputUtcMs(trimmed);
    if (ms === null) {
      draft = toDisplay(value);
      return;
    }
    const normalized = formatStartInputUtc(ms);
    draft = normalized;
    emitIfChanged(normalized, ms);
  }

  function emitIfChanged(nextValue: string, nextMs: number | null): void {
    const prevMs = value ? parseStartInputUtcMs(value) : null;
    if (nextValue === value && nextMs === prevMs) return;
    dispatch("change", { value: nextValue, ms: nextMs });
  }

  function adjustSegment(id: SegmentId, delta: number): void {
    const text = ensureSegmentedText();
    const parsed = parseStartInputUtcMs(text);
    const date = parsed === null ? new Date() : new Date(parsed);
    if (id === "day") date.setUTCDate(date.getUTCDate() + delta);
    else if (id === "month") date.setUTCMonth(date.getUTCMonth() + delta);
    else if (id === "year") date.setUTCFullYear(date.getUTCFullYear() + delta);
    else if (id === "hour") date.setUTCHours(date.getUTCHours() + delta);
    else date.setUTCMinutes(date.getUTCMinutes() + delta);
    draft = formatStartInputUtc(date.getTime());
    resetDigitBuffer();
    commit(draft);
  }

  function clearSegment(id: SegmentId): void {
    const text = ensureSegmentedText();
    const segment = segmentById(id);
    draft = replaceSegment(text, segment, segment.placeholder);
    resetDigitBuffer();
  }

  function writeDigit(id: SegmentId, digit: string): void {
    const text = ensureSegmentedText();
    const segment = segmentById(id);
    const now = Date.now();
    if (digitBufferSegment !== id || now - digitBufferTs > 1200) {
      digitBuffer = "";
    }
    digitBufferSegment = id;
    digitBufferTs = now;
    digitBuffer = (digitBuffer + digit).slice(-segment.placeholder.length);
    const padded = digitBuffer.padStart(segment.placeholder.length, "0");
    draft = replaceSegment(text, segment, padded);

    if (digitBuffer.length >= segment.placeholder.length) {
      const next = nextSegment(id);
      if (next) selectSegmentSoon(next.id);
      else selectSegmentSoon(id);
      resetDigitBuffer();
      return;
    }
    selectSegmentSoon(id);
  }

  function ensureSegmentedText(): string {
    const source = draft.trim();
    if (!source) return MASK;
    if (MASK_RE.test(source)) return source;
    const normalized = normalizeStartInput(source);
    if (normalized) return normalized;
    return MASK;
  }

  function selectSegmentAtCaret(): void {
    const seg = activeSegment() ?? SEGMENTS[0];
    selectSegment(seg);
  }

  function selectSegmentById(id: SegmentId): void {
    selectSegment(segmentById(id));
  }

  function selectSegment(segment: Segment): void {
    if (!inputEl) return;
    inputEl.setSelectionRange(segment.start, segment.end);
  }

  function activeSegment(): Segment | null {
    if (!inputEl) return null;
    const selStart = inputEl.selectionStart ?? 0;
    const selEnd = inputEl.selectionEnd ?? selStart;
    const exact = SEGMENTS.find((seg) => seg.start === selStart && seg.end === selEnd);
    if (exact) return exact;
    const caret = selStart;
    return SEGMENTS.find((seg) => caret >= seg.start && caret <= seg.end) ?? null;
  }

  function nextSegment(id: SegmentId): Segment | null {
    const idx = SEGMENTS.findIndex((seg) => seg.id === id);
    if (idx === -1 || idx >= SEGMENTS.length - 1) return null;
    return SEGMENTS[idx + 1];
  }

  function segmentById(id: SegmentId): Segment {
    const found = SEGMENTS.find((seg) => seg.id === id);
    if (!found) return SEGMENTS[0];
    return found;
  }

  function replaceSegment(text: string, segment: Segment, replacement: string): string {
    return text.slice(0, segment.start) + replacement + text.slice(segment.end);
  }

  function toDisplay(source: string): string {
    if (!source) return "";
    return normalizeStartInput(source) ?? source.trim();
  }

  function resetDigitBuffer(): void {
    digitBuffer = "";
    digitBufferSegment = null;
    digitBufferTs = 0;
  }

  function selectSegmentSoon(id: SegmentId): void {
    void tick().then(() => selectSegmentById(id));
  }

  function selectCurrentSegmentSoon(): void {
    void tick().then(selectSegmentAtCaret);
  }

  function openCalendarPicker(): void {
    const source = draft || value;
    const picker = calendarEl as (HTMLInputElement & { showPicker?: () => void }) | null;
    if (!picker) return;
    picker.value = toDateTimeLocalValue(source);
    if (typeof picker.showPicker === "function") {
      picker.showPicker();
      return;
    }
    picker.focus();
    picker.click();
  }

  function handleCalendarInput(event: Event): void {
    const picked = (event.target as HTMLInputElement).value;
    if (!picked) return;
    commit(picked);
  }

  function handleCalendarChange(event: Event): void {
    const picked = (event.target as HTMLInputElement).value;
    if (!picked) return;
    commit(picked);
  }

  function toDateTimeLocalValue(source: string): string {
    const ms = parseStartInputUtcMs(source);
    if (ms === null) return "";
    const d = new Date(ms);
    return `${d.getUTCFullYear()}-${pad2(d.getUTCMonth() + 1)}-${pad2(d.getUTCDate())}T${pad2(d.getUTCHours())}:${pad2(d.getUTCMinutes())}`;
  }

  function pad2(value: number): string {
    return value < 10 ? `0${value}` : `${value}`;
  }
</script>

<div class="relative">
  <input
    bind:this={inputEl}
    class={`${className} pr-8`}
    type="text"
    inputmode="numeric"
    spellcheck="false"
    {placeholder}
    value={draft}
    on:mousedown={handlePointerDown}
    on:input={handleInput}
    on:focus={handleFocus}
    on:blur={handleBlur}
    on:click={handleClick}
    on:keydown={handleKeydown}
    on:paste={handlePaste}
  />
  <button
    class="absolute right-1 top-1/2 -translate-y-1/2 rounded p-1 text-slate-300 hover:bg-slate-800/80 hover:text-slate-100"
    type="button"
    aria-label="Open calendar"
    on:click={openCalendarPicker}
  >
    <svg viewBox="0 0 20 20" fill="none" class="h-4 w-4" aria-hidden="true">
      <rect x="3" y="4.5" width="14" height="12" rx="2" stroke="currentColor" stroke-width="1.5" />
      <path d="M3 8.5h14M6.5 3v3M13.5 3v3" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" />
    </svg>
  </button>
  <input
    bind:this={calendarEl}
    class="pointer-events-none absolute h-0 w-0 opacity-0"
    type="datetime-local"
    tabindex="-1"
    aria-hidden="true"
    on:input={handleCalendarInput}
    on:change={handleCalendarChange}
  />
</div>
