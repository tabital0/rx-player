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

import type { IManifestLoader, ILoadedManifestFormat } from "../../public_types";
import { assertUnreachable } from "../../utils/assert";
import request from "../../utils/request";
import type { CancellationSignal } from "../../utils/task_canceller";
import type {
  IManifestLoaderOptions,
  IRequestedData,
  ITransportManifestPipeline,
} from "../types";
import addQueryString from "./add_query_string";
import callCustomManifestLoader from "./call_custom_manifest_loader";

/**
 * Manifest loader triggered if there was no custom-defined one in the API.
 * @param {string} preferredType
 * @returns {Function}
 */
function generateRegularManifestLoader(
  preferredType: "arraybuffer" | "text" | "document",
): (
  initialUrl: string | undefined,
  loaderOptions: IManifestLoaderOptions,
  cancelSignal: CancellationSignal,
) => Promise<IRequestedData<ILoadedManifestFormat>> {
  return function regularManifestLoader(
    initialUrl: string | undefined,
    loaderOptions: IManifestLoaderOptions,
    cancelSignal: CancellationSignal,
  ): Promise<IRequestedData<ILoadedManifestFormat>> {
    if (initialUrl === undefined) {
      throw new Error("Cannot perform HTTP(s) request. URL not known");
    }

    const url =
      loaderOptions.cmcdPayload?.type === "query"
        ? addQueryString(initialUrl, loaderOptions.cmcdPayload.value)
        : initialUrl;

    const cmcdHeaders =
      loaderOptions.cmcdPayload?.type === "headers"
        ? loaderOptions.cmcdPayload.value
        : undefined;

    // What follows could be written in a single line, but TypeScript wouldn't
    // shut up.
    // So I wrote that instead, temporarily of course ;)
    switch (preferredType) {
      case "arraybuffer":
        return request({
          url,
          headers: cmcdHeaders,
          responseType: "arraybuffer",
          timeout: loaderOptions.timeout,
          connectionTimeout: loaderOptions.connectionTimeout,
          cancelSignal,
        });
      case "text":
        return request({
          url,
          headers: cmcdHeaders,
          responseType: "text",
          timeout: loaderOptions.timeout,
          connectionTimeout: loaderOptions.connectionTimeout,
          cancelSignal,
        });
      case "document":
        return request({
          url,
          headers: cmcdHeaders,
          responseType: "document",
          timeout: loaderOptions.timeout,
          connectionTimeout: loaderOptions.connectionTimeout,
          cancelSignal,
        });
      default:
        assertUnreachable(preferredType);
    }
  };
}

/**
 * Generate a manifest loader for the application
 * @param {Function} [customManifestLoader]
 * @param {string} preferredType
 * @returns {Function}
 */
export default function generateManifestLoader(
  { customManifestLoader }: { customManifestLoader?: IManifestLoader | undefined },
  preferredType: "arraybuffer" | "text" | "document",
  integrityCheck:
    | ((
        loader: ITransportManifestPipeline["loadManifest"],
      ) => ITransportManifestPipeline["loadManifest"])
    | null,
): ITransportManifestPipeline["loadManifest"] {
  const regularManifestLoader = generateRegularManifestLoader(preferredType);
  const actualLoader =
    typeof customManifestLoader !== "function"
      ? regularManifestLoader
      : callCustomManifestLoader(customManifestLoader, regularManifestLoader);
  return integrityCheck !== null ? integrityCheck(actualLoader) : actualLoader;
}
