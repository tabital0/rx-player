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
/**
 * /!\ This file is feature-switchable.
 * It always should be imported through the `features` object.
 */
import { Observable } from "rxjs";
import { IReadOnlySharedReference } from "../../utils/reference";
import { PlaybackObserver } from "../api";
import { IKeySystemOption } from "../eme";
import { IInitialTimeOptions } from "./get_initial_time";
import { IDirectfileEvent } from "./types";
export interface IDirectFileOptions {
    autoPlay: boolean;
    keySystems: IKeySystemOption[];
    mediaElement: HTMLMediaElement;
    playbackObserver: PlaybackObserver;
    speed: IReadOnlySharedReference<number>;
    startAt?: IInitialTimeOptions;
    url?: string;
}
/**
 * Launch a content in "Directfile mode".
 * @param {Object} directfileOptions
 * @returns {Observable}
 */
export default function initializeDirectfileContent({ autoPlay, keySystems, mediaElement, playbackObserver, speed, startAt, url, }: IDirectFileOptions): Observable<IDirectfileEvent>;
export { IDirectfileEvent };