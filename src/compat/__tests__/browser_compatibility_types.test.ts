import { describe, beforeEach, it, expect, vi } from "vitest";
import globalScope from "../../utils/global_scope";

// Needed for calling require (which itself is needed to mock properly) because
// it is not type-checked:
/* eslint-disable @typescript-eslint/no-unsafe-member-access */
/* eslint-disable @typescript-eslint/no-unsafe-call */
/* eslint-disable @typescript-eslint/no-unsafe-assignment */
/* eslint-disable @typescript-eslint/no-unsafe-return */

describe("compat - browser compatibility types", () => {
  interface IFakeWindow {
    MediaSource?: unknown;
    MozMediaSource?: unknown;
    WebKitMediaSource?: unknown;
    MSMediaSource?: unknown;
  }
  const gs = globalScope as IFakeWindow;
  beforeEach(() => {
    vi.resetModules();
  });

  it("should use the native MediaSource if defined", async () => {
    vi.doMock("../../utils/is_node", () => ({
      default: false,
    }));

    const origMediaSource = gs.MediaSource;
    const origMozMediaSource = gs.MozMediaSource;
    const origWebKitMediaSource = gs.WebKitMediaSource;
    const origMSMediaSource = gs.MSMediaSource;

    gs.MediaSource = { a: 1 };
    gs.MozMediaSource = { a: 2 };
    gs.WebKitMediaSource = { a: 3 };
    gs.MSMediaSource = { a: 4 };

    const { MediaSource_ } = await vi.importActual("../browser_compatibility_types");
    expect(MediaSource_).toEqual({ a: 1 });

    gs.MediaSource = origMediaSource;
    gs.MozMediaSource = origMozMediaSource;
    gs.WebKitMediaSource = origWebKitMediaSource;
    gs.MSMediaSource = origMSMediaSource;
  });

  it("should use MozMediaSource if defined and MediaSource is not", async () => {
    vi.doMock("../../utils/is_node", () => ({
      default: false,
    }));

    const origMediaSource = gs.MediaSource;
    const origMozMediaSource = gs.MozMediaSource;
    const origWebKitMediaSource = gs.WebKitMediaSource;
    const origMSMediaSource = gs.MSMediaSource;

    gs.MediaSource = undefined;
    gs.MozMediaSource = { a: 2 };
    gs.WebKitMediaSource = undefined;
    gs.MSMediaSource = undefined;

    const { MediaSource_ } = await vi.importActual("../browser_compatibility_types");
    expect(MediaSource_).toEqual({ a: 2 });

    gs.MediaSource = origMediaSource;
    gs.MozMediaSource = origMozMediaSource;
    gs.WebKitMediaSource = origWebKitMediaSource;
    gs.MSMediaSource = origMSMediaSource;
  });

  it("should use WebKitMediaSource if defined and MediaSource is not", async () => {
    vi.doMock("../../utils/is_node", () => ({
      default: false,
    }));

    const origMediaSource = gs.MediaSource;
    const origMozMediaSource = gs.MozMediaSource;
    const origWebKitMediaSource = gs.WebKitMediaSource;
    const origMSMediaSource = gs.MSMediaSource;

    gs.MediaSource = undefined;
    gs.MozMediaSource = undefined;
    gs.WebKitMediaSource = { a: 3 };
    gs.MSMediaSource = undefined;

    const { MediaSource_ } = await vi.importActual("../browser_compatibility_types");
    expect(MediaSource_).toEqual({ a: 3 });

    gs.MediaSource = origMediaSource;
    gs.MozMediaSource = origMozMediaSource;
    gs.WebKitMediaSource = origWebKitMediaSource;
    gs.MSMediaSource = origMSMediaSource;
  });

  it("should use MSMediaSource if defined and MediaSource is not", async () => {
    vi.doMock("../../utils/is_node", () => ({
      default: false,
    }));

    const origMediaSource = gs.MediaSource;
    const origMozMediaSource = gs.MozMediaSource;
    const origWebKitMediaSource = gs.WebKitMediaSource;
    const origMSMediaSource = gs.MSMediaSource;

    gs.MediaSource = undefined;
    gs.MozMediaSource = undefined;
    gs.WebKitMediaSource = undefined;
    gs.MSMediaSource = { a: 4 };

    const { MediaSource_ } = await vi.importActual("../browser_compatibility_types");
    expect(MediaSource_).toEqual({ a: 4 });

    gs.MediaSource = origMediaSource;
    gs.MozMediaSource = origMozMediaSource;
    gs.WebKitMediaSource = origWebKitMediaSource;
    gs.MSMediaSource = origMSMediaSource;
  });
});
