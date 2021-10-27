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
import { combineLatest as observableCombineLatest, defer as observableDefer, merge as observableMerge, of as observableOf, } from "rxjs";
import { filter, ignoreElements, map, startWith, switchMap, tap, withLatestFrom, } from "rxjs/operators";
import log from "../../log";
import { getLeftSizeOfRange } from "../../utils/ranges";
import BufferBasedChooser from "./buffer_based_chooser";
import filterByBitrate from "./filter_by_bitrate";
import filterByWidth from "./filter_by_width";
import NetworkAnalyzer from "./network_analyzer";
import PendingRequestsStore from "./pending_requests_store";
import RepresentationScoreCalculator from "./representation_score_calculator";
import selectOptimalRepresentation from "./select_optimal_representation";
/**
 * Filter representations given through filters options.
 * @param {Array.<Representation>} representations
 * @param {Object} filters - Filter Object.
 * @returns {Array.<Representation>}
 */
function getFilteredRepresentations(representations, filters) {
    var _representations = representations;
    if (filters.bitrate != null) {
        _representations = filterByBitrate(_representations, filters.bitrate);
    }
    if (filters.width != null) {
        _representations = filterByWidth(_representations, filters.width);
    }
    return _representations;
}
/**
 * Estimate regularly the current network bandwidth and the best Representation
 * that can be played according to the current network and playback conditions.
 *
 * A `RepresentationEstimator` only does estimations for a given type (e.g.
 * "audio", "video" etc.) and Period.
 *
 * If estimates for multiple types and/or Periods are needed, you should
 * create as many `RepresentationEstimator`.
 * @param {Object} args
 * @returns {Observable}
 */
export default function RepresentationEstimator(_a) {
    var bandwidthEstimator = _a.bandwidthEstimator, clock$ = _a.clock$, filters$ = _a.filters$, initialBitrate = _a.initialBitrate, lowLatencyMode = _a.lowLatencyMode, manualBitrate$ = _a.manualBitrate$, minAutoBitrate$ = _a.minAutoBitrate$, maxAutoBitrate$ = _a.maxAutoBitrate$, representations = _a.representations, streamEvents$ = _a.streamEvents$;
    var scoreCalculator = new RepresentationScoreCalculator();
    var networkAnalyzer = new NetworkAnalyzer(initialBitrate == null ? 0 :
        initialBitrate, lowLatencyMode);
    var requestsStore = new PendingRequestsStore();
    /**
     * Callback to call when new metrics are available
     * @param {Object} value
     */
    function onMetric(value) {
        var duration = value.duration, size = value.size, content = value.content;
        // calculate bandwidth
        bandwidthEstimator.addSample(duration, size);
        // calculate "maintainability score"
        var segment = content.segment;
        var requestDuration = duration / 1000;
        var segmentDuration = segment.duration;
        var representation = content.representation;
        scoreCalculator.addSample(representation, requestDuration, segmentDuration);
    }
    var metrics$ = streamEvents$.pipe(filter(function (e) { return e.type === "metrics"; }), tap(function (_a) {
        var value = _a.value;
        return onMetric(value);
    }), ignoreElements());
    var requests$ = streamEvents$.pipe(tap(function (evt) {
        switch (evt.type) {
            case "requestBegin":
                requestsStore.add(evt.value);
                break;
            case "requestEnd":
                requestsStore.remove(evt.value.id);
                break;
            case "progress":
                requestsStore.addProgress(evt.value);
                break;
        }
    }), ignoreElements());
    var currentRepresentation$ = streamEvents$.pipe(filter(function (e) { return e.type === "representationChange"; }), map(function (e) { return e.value.representation; }), startWith(null));
    var estimate$ = observableDefer(function () {
        if (representations.length === 0) {
            throw new Error("ABRManager: no representation choice given");
        }
        if (representations.length === 1) {
            return observableOf({ bitrate: undefined,
                representation: representations[0],
                manual: false,
                urgent: true,
                knownStableBitrate: undefined });
        }
        return manualBitrate$.pipe(switchMap(function (manualBitrate) {
            if (manualBitrate >= 0) {
                // -- MANUAL mode --
                var manualRepresentation = selectOptimalRepresentation(representations, manualBitrate, 0, Infinity);
                return observableOf({
                    representation: manualRepresentation,
                    bitrate: undefined,
                    knownStableBitrate: undefined,
                    manual: true,
                    urgent: true, // a manual bitrate switch should happen immediately
                });
            }
            // -- AUTO mode --
            var lastEstimatedBitrate;
            var forceBandwidthMode = true;
            // Emit each time a buffer-based estimation should be actualized (each
            // time a segment is added).
            var bufferBasedClock$ = streamEvents$.pipe(filter(function (e) { return e.type === "added-segment"; }), withLatestFrom(clock$), map(function (_a) {
                var evtValue = _a[0].value, _b = _a[1], speed = _b.speed, position = _b.position;
                var timeRanges = evtValue.buffered;
                var bufferGap = getLeftSizeOfRange(timeRanges, position);
                var representation = evtValue.content.representation;
                var currentScore = scoreCalculator.getEstimate(representation);
                var currentBitrate = representation.bitrate;
                return { bufferGap: bufferGap, currentBitrate: currentBitrate, currentScore: currentScore, speed: speed };
            }));
            var bitrates = representations.map(function (r) { return r.bitrate; });
            var bufferBasedEstimation$ = BufferBasedChooser(bufferBasedClock$, bitrates)
                .pipe(startWith(undefined));
            return observableCombineLatest([clock$,
                minAutoBitrate$,
                maxAutoBitrate$,
                filters$,
                bufferBasedEstimation$]).pipe(withLatestFrom(currentRepresentation$), map(function (_a) {
                var _b = _a[0], clock = _b[0], minAutoBitrate = _b[1], maxAutoBitrate = _b[2], filters = _b[3], bufferBasedBitrate = _b[4], currentRepresentation = _a[1];
                var _representations = getFilteredRepresentations(representations, filters);
                var requests = requestsStore.getRequests();
                var _c = networkAnalyzer
                    .getBandwidthEstimate(clock, bandwidthEstimator, currentRepresentation, requests, lastEstimatedBitrate), bandwidthEstimate = _c.bandwidthEstimate, bitrateChosen = _c.bitrateChosen;
                lastEstimatedBitrate = bandwidthEstimate;
                var stableRepresentation = scoreCalculator.getLastStableRepresentation();
                var knownStableBitrate = stableRepresentation == null ?
                    undefined :
                    stableRepresentation.bitrate / (clock.speed > 0 ? clock.speed : 1);
                var bufferGap = clock.bufferGap;
                if (!forceBandwidthMode && bufferGap <= 5) {
                    forceBandwidthMode = true;
                }
                else if (forceBandwidthMode && isFinite(bufferGap) && bufferGap > 10) {
                    forceBandwidthMode = false;
                }
                var chosenRepFromBandwidth = selectOptimalRepresentation(_representations, bitrateChosen, minAutoBitrate, maxAutoBitrate);
                if (forceBandwidthMode) {
                    log.debug("ABR: Choosing representation with bandwidth estimation.", chosenRepFromBandwidth);
                    return { bitrate: bandwidthEstimate,
                        representation: chosenRepFromBandwidth,
                        urgent: networkAnalyzer.isUrgent(chosenRepFromBandwidth.bitrate, currentRepresentation, requests, clock),
                        manual: false, knownStableBitrate: knownStableBitrate };
                }
                if (bufferBasedBitrate == null ||
                    chosenRepFromBandwidth.bitrate >= bufferBasedBitrate) {
                    log.debug("ABR: Choosing representation with bandwidth estimation.", chosenRepFromBandwidth);
                    return { bitrate: bandwidthEstimate,
                        representation: chosenRepFromBandwidth,
                        urgent: networkAnalyzer.isUrgent(chosenRepFromBandwidth.bitrate, currentRepresentation, requests, clock),
                        manual: false, knownStableBitrate: knownStableBitrate };
                }
                var chosenRepresentation = selectOptimalRepresentation(_representations, bufferBasedBitrate, minAutoBitrate, maxAutoBitrate);
                if (bufferBasedBitrate <= maxAutoBitrate) {
                    log.debug("ABR: Choosing representation with buffer based bitrate ceiling.", chosenRepresentation);
                }
                return { bitrate: bandwidthEstimate,
                    representation: chosenRepresentation,
                    urgent: networkAnalyzer.isUrgent(bufferBasedBitrate, currentRepresentation, requests, clock),
                    manual: false, knownStableBitrate: knownStableBitrate };
            }));
        }));
    });
    return observableMerge(metrics$, requests$, estimate$);
}