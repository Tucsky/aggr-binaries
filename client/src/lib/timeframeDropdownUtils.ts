export type TimeframeEntry = { value: string; ms: number };

export function filterTimeframesByQuery(
  list: TimeframeEntry[],
  query: string,
): TimeframeEntry[] {
  const normalizedQuery = query.trim().toLowerCase();
  if (!normalizedQuery) return list;
  return list.filter((entry) =>
    entry.value.toLowerCase().includes(normalizedQuery),
  );
}
