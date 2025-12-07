import { writable } from "svelte/store";

export type ToastLevel = "info" | "success" | "error";

export interface Toast {
  id: number;
  message: string;
  level: ToastLevel;
  ts: number;
}

const toasts = writable<Toast[]>([]);
let counter = 0;

export function addToast(message: string, level: ToastLevel = "info", durationMs = 3000): void {
  console.log(message)
  const id = ++counter;
  const toast: Toast = { id, message, level, ts: Date.now() };
  toasts.update((list) => [...list, toast]);
  if (durationMs > 0) {
    setTimeout(() => {
      toasts.update((list) => list.filter((t) => t.id !== id));
    }, durationMs);
  }
}

export default toasts;
