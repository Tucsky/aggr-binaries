<script lang="ts" context="module">
  export type RectLike = {
    top: number;
    left: number;
    width: number;
    height: number;
  };
</script>

<script lang="ts">
  import { createEventDispatcher, onDestroy, onMount, tick } from "svelte";
  import { fade } from "svelte/transition";

  export let open = false;
  export let anchorEl: HTMLElement | null = null;
  export let margin = 12;

  const dispatch = createEventDispatcher<{ close: void }>();

  let el: HTMLDivElement | null = null;
  let top = 0;
  let left = 0;
  let hasPosition = false;

  let unregister: (() => void) | null = null;

  let raf = 0;
  let inFlight = false;

  function schedulePosition() {
    if (!open || !anchorEl) return;
    if (raf) return; // already scheduled this frame
    raf = requestAnimationFrame(() => {
      raf = 0;
      void position(); // fire and forget; position() guards itself
    });
  }
  $: if (open && anchorEl) {
    setupListeners();
    schedulePosition();
  } else {
    teardownListeners();
    hasPosition = false;
  }

  onMount(() => teardownListeners);
  onDestroy(teardownListeners);

  function setupListeners() {
    teardownListeners();

    const handler = (ev: Event) => {
      const target = ev.target as HTMLElement | null;
      if (!el || !target) return;
      if (el.contains(target)) return;
      if (anchorEl && anchorEl.contains(target)) return;
      dispatch("close");
    };

    document.addEventListener("mousedown", handler);
    document.addEventListener("touchstart", handler, { passive: true });

    const resizeHandler = () => schedulePosition();
    window.addEventListener("resize", resizeHandler);

    unregister = () => {
      document.removeEventListener("mousedown", handler);
      document.removeEventListener("touchstart", handler);
      window.removeEventListener("resize", resizeHandler);
    };
  }

  function teardownListeners() {
    if (unregister) {
      unregister();
      unregister = null;
    }
  }

  async function position() {
    if (inFlight) {
      return; // prevents tick()-chains if schedulePosition fires fast
    }
    inFlight = true;

    try {
      if (!open || !anchorEl || !el) {
        return;
      }

      await tick();
      const a = anchorEl.getBoundingClientRect();
      const d = el.getBoundingClientRect();
      const vw = window.innerWidth;
      const vh = window.innerHeight;

      const spaces = {
        bottom: vh - a.bottom - margin,
        top: a.top - margin,
        right: vw - a.right - margin,
        left: a.left - margin,
      };

      const placements = [
        {
          name: "bottom",
          fits: spaces.bottom >= d.height,
          top: a.bottom + margin,
          left: a.left + a.width / 2 - d.width / 2,
          availH: spaces.bottom,
        },
        {
          name: "top",
          fits: spaces.top >= d.height,
          top: a.top - d.height - margin,
          left: a.left + a.width / 2 - d.width / 2,
          availH: spaces.top,
        },
        {
          name: "right",
          fits: spaces.right >= d.width,
          top: a.top + a.height / 2 - d.height / 2,
          left: a.right + margin,
          availH: vh - margin * 2,
        },
        {
          name: "left",
          fits: spaces.left >= d.width,
          top: a.top + a.height / 2 - d.height / 2,
          left: a.left - d.width - margin,
          availH: vh - margin * 2,
        },
      ];

      const best =
        placements.find((p) => p.fits) ??
        placements.slice().sort((p, q) => q.availH - p.availH)[0];

      let t = best.top;
      let l = best.left;

      // clamp
      t = Math.max(margin, Math.min(t, vh - d.height - margin));
      l = Math.max(margin, Math.min(l, vw - d.width - margin));

      top = Math.round(t);
      left = Math.round(l);

      // optional: dynamically constrain height based on chosen side
      // (you can bind this to a variable too instead of touching el.style directly)
      el.style.maxHeight = `${Math.max(80, Math.floor(best.availH))}px`;

      hasPosition = true;
    } finally {
      inFlight = false;
    }
  }
</script>

{#if open && anchorEl}
  <div
    class={`fixed z-50 shadow-[0_18px_50px_-10px_rgba(0,0,0,0.6)]`}
    bind:this={el}
    style={`top:${top}px;left:${left}px;max-height:70vh;max-width:320px;`}
    transition:fade={{ duration: 120 }}
  >
    <div
      class={`rounded-lg border border-slate-800 bg-slate-900/95 overflow-y-auto max-h-[70vh]`}
    >
      <slot />
    </div>
  </div>
{/if}
