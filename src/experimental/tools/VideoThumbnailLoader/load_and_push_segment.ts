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

import type {
  ISegmentFetcher,
  ISegmentLoaderContent,
} from "../../../core/fetchers/segment/segment_fetcher";
import log from "../../../log";
import type { MainSourceBufferInterface } from "../../../mse/main_media_source_interface";
import type { CancellationSignal } from "../../../utils/task_canceller";

/**
 * @param {Object} segmentInfo
 * @param {Object} sourceBufferInterface
 * @param {Object} segmentFetcher
 * @param {Object} cancelSignal
 * @returns {Promise}
 */
export default function loadAndPushSegment(
  segmentInfo: ISegmentLoaderContent,
  sourceBufferInterface: MainSourceBufferInterface,
  segmentFetcher: ISegmentFetcher<ArrayBuffer | Uint8Array>,
  cancelSignal: CancellationSignal,
): Promise<unknown> {
  const pushOperations: Array<Promise<unknown>> = [];
  return segmentFetcher(
    segmentInfo,
    {
      onChunk(parseChunk) {
        const parsed = parseChunk(undefined);
        let data: BufferSource | null;
        let timestampOffset: number;
        const codec = segmentInfo.representation.getMimeTypeString();
        if (parsed.segmentType === "init") {
          data = parsed.initializationData;
          timestampOffset = 0;
        } else {
          data = parsed.chunkData;
          timestampOffset = parsed.chunkOffset;
        }
        let pushOperation;
        if (data === null) {
          pushOperation = Promise.resolve(null);
        } else {
          pushOperation = sourceBufferInterface.appendBuffer(data, {
            appendWindow: [segmentInfo.period.start, segmentInfo.period.end],
            timestampOffset,
            codec,
          });
        }
        pushOperations.push(pushOperation);
      },
      onAllChunksReceived() {
        return;
      },
      onRetry(error) {
        log.warn("Retrying segment request", error);
      },
    },
    cancelSignal,
  ).then(() => Promise.all(pushOperations));
}
