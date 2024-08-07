/**
 * Copyright 2015 CANAL+ Group
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { CustomLoaderError } from "../../errors";
import type { ICdnMetadata } from "../../parsers/manifest";
import type { ISegmentLoader as ICustomSegmentLoader } from "../../public_types";
import request, { fetchIsSupported } from "../../utils/request";
import type { CancellationError, CancellationSignal } from "../../utils/task_canceller";
import warnOnce from "../../utils/warn_once";
import type {
  ILoadedAudioVideoSegmentFormat,
  ISegmentContext,
  ISegmentLoader,
  ISegmentLoaderCallbacks,
  ISegmentLoaderOptions,
  ISegmentLoaderResultChunkedComplete,
  ISegmentLoaderResultSegmentCreated,
  ISegmentLoaderResultSegmentLoaded,
} from "../types";
import addQueryString from "../utils/add_query_string";
import byteRange from "../utils/byte_range";
import inferSegmentContainer from "../utils/infer_segment_container";
import constructSegmentUrl from "./construct_segment_url";
import initSegmentLoader from "./init_segment_loader";
import { addSegmentIntegrityChecks } from "./integrity_checks";
import loadChunkedSegmentData from "./load_chunked_segment_data";

/**
 * Segment loader triggered if there was no custom-defined one in the API.
 * @param {string} initialUrl
 * @param {Object} context
 * @param {boolean} lowLatencyMode
 * @param {Object} options
 * @param {Object} callbacks
 * @param {Object} cancelSignal
 * @returns {Promise}
 */
export async function regularSegmentLoader(
  initialUrl: string,
  context: ISegmentContext,
  lowLatencyMode: boolean,
  options: ISegmentLoaderOptions,
  callbacks: ISegmentLoaderCallbacks<ILoadedAudioVideoSegmentFormat>,
  cancelSignal: CancellationSignal,
): Promise<
  | ISegmentLoaderResultSegmentLoaded<ILoadedAudioVideoSegmentFormat>
  | ISegmentLoaderResultSegmentCreated<ILoadedAudioVideoSegmentFormat>
  | ISegmentLoaderResultChunkedComplete
> {
  if (context.segment.isInit) {
    return initSegmentLoader(
      initialUrl,
      context.segment,
      options,
      cancelSignal,
      callbacks,
    );
  }

  const url =
    options.cmcdPayload?.type === "query"
      ? addQueryString(initialUrl, options.cmcdPayload.value)
      : initialUrl;

  const cmcdHeaders =
    options.cmcdPayload?.type === "headers" ? options.cmcdPayload.value : undefined;

  const { segment } = context;
  let headers;
  if (segment.range !== undefined) {
    headers = {
      ...cmcdHeaders,
      Range: byteRange(segment.range),
    };
  } else if (cmcdHeaders !== undefined) {
    headers = cmcdHeaders;
  }

  const containerType = inferSegmentContainer(context.type, context.mimeType);
  if (lowLatencyMode && (containerType === "mp4" || containerType === undefined)) {
    if (fetchIsSupported()) {
      return loadChunkedSegmentData(
        url,
        {
          headers,
          timeout: options.timeout,
          connectionTimeout: options.connectionTimeout,
        },
        callbacks,
        cancelSignal,
      );
    } else {
      warnOnce(
        "DASH: Your browser does not have the fetch API. You will have " +
          "a higher chance of rebuffering when playing close to the live edge",
      );
    }
  }

  const data = await request({
    url,
    responseType: "arraybuffer",
    headers,
    timeout: options.timeout,
    connectionTimeout: options.connectionTimeout,
    cancelSignal,
    onProgress: callbacks.onProgress,
  });
  return { resultType: "segment-loaded", resultData: data };
}

/**
 * @param {Object} config
 * @returns {Function}
 */
export default function generateSegmentLoader({
  lowLatencyMode,
  segmentLoader: customSegmentLoader,
  checkMediaSegmentIntegrity,
}: {
  lowLatencyMode: boolean;
  segmentLoader?: ICustomSegmentLoader | undefined;
  checkMediaSegmentIntegrity?: boolean | undefined;
}): ISegmentLoader<Uint8Array | ArrayBuffer | null> {
  return checkMediaSegmentIntegrity !== true
    ? segmentLoader
    : addSegmentIntegrityChecks(segmentLoader);

  /**
   * @param {Object|null} wantedCdn
   * @param {Object} context
   * @param {Object} options
   * @param {Object} cancelSignal
   * @param {Object} callbacks
   * @returns {Promise.<Object>}
   */
  function segmentLoader(
    wantedCdn: ICdnMetadata | null,
    context: ISegmentContext,
    options: ISegmentLoaderOptions,
    cancelSignal: CancellationSignal,
    callbacks: ISegmentLoaderCallbacks<Uint8Array | ArrayBuffer | null>,
  ): Promise<
    | ISegmentLoaderResultSegmentLoaded<ILoadedAudioVideoSegmentFormat>
    | ISegmentLoaderResultSegmentCreated<ILoadedAudioVideoSegmentFormat>
    | ISegmentLoaderResultChunkedComplete
  > {
    const url = constructSegmentUrl(wantedCdn, context.segment);
    if (url === null) {
      return Promise.resolve({
        resultType: "segment-created",
        resultData: null,
      });
    }

    if (lowLatencyMode || customSegmentLoader === undefined) {
      return regularSegmentLoader(
        url,
        context,
        lowLatencyMode,
        options,
        callbacks,
        cancelSignal,
      );
    }

    return new Promise((res, rej) => {
      /** `true` when the custom segmentLoader should not be active anymore. */
      let hasFinished = false;

      /**
       * Callback triggered when the custom segment loader has a response.
       * @param {Object} _args
       */
      const resolve = (_args: {
        data: ArrayBuffer | Uint8Array;
        size?: number | undefined;
        duration?: number | undefined;
      }) => {
        if (hasFinished || cancelSignal.isCancelled()) {
          return;
        }
        hasFinished = true;
        cancelSignal.deregister(abortCustomLoader);
        res({
          resultType: "segment-loaded",
          resultData: {
            responseData: _args.data,
            size: _args.size,
            requestDuration: _args.duration,
          },
        });
      };

      /**
       * Callback triggered when the custom segment loader fails
       * @param {*} err - The corresponding error encountered
       */
      const reject = (err: unknown): void => {
        if (hasFinished || cancelSignal.isCancelled()) {
          return;
        }
        hasFinished = true;
        cancelSignal.deregister(abortCustomLoader);

        // Format error and send it
        const castedErr = err as
          | null
          | undefined
          | { message?: string; canRetry?: boolean; xhr?: XMLHttpRequest };
        const message =
          castedErr?.message ??
          "Unknown error when fetching a DASH segment through a " +
            "custom segmentLoader.";
        const emittedErr = new CustomLoaderError(
          message,
          castedErr?.canRetry ?? false,
          castedErr?.xhr,
        );
        rej(emittedErr);
      };

      const progress = (_args: {
        duration: number;
        size: number;
        totalSize?: number | undefined;
      }) => {
        if (hasFinished || cancelSignal.isCancelled()) {
          return;
        }
        callbacks.onProgress({
          duration: _args.duration,
          size: _args.size,
          totalSize: _args.totalSize,
        });
      };

      /**
       * Callback triggered when the custom segment loader wants to fallback to
       * the "regular" implementation
       */
      const fallback = () => {
        if (hasFinished || cancelSignal.isCancelled()) {
          return;
        }
        hasFinished = true;
        cancelSignal.deregister(abortCustomLoader);
        regularSegmentLoader(
          url,
          context,
          lowLatencyMode,
          options,
          callbacks,
          cancelSignal,
        ).then(res, rej);
      };

      const customCallbacks = { reject, resolve, progress, fallback };

      let byteRanges: Array<[number, number]> | undefined;
      if (context.segment.range !== undefined) {
        byteRanges = [context.segment.range];
        if (context.segment.indexRange !== undefined) {
          byteRanges.push(context.segment.indexRange);
        }
      }
      const args = {
        isInit: context.segment.isInit,
        timeout: options.timeout,
        byteRanges,
        trackType: context.type,
        url,
        cmcdPayload: options.cmcdPayload,
      };
      const abort = customSegmentLoader(args, customCallbacks);

      cancelSignal.register(abortCustomLoader);

      /**
       * The logic to run when the custom loader is cancelled while pending.
       * @param {Error} err
       */
      function abortCustomLoader(err: CancellationError) {
        if (hasFinished) {
          return;
        }
        hasFinished = true;
        if (typeof abort === "function") {
          abort();
        }
        rej(err);
      }
    });
  }
}
