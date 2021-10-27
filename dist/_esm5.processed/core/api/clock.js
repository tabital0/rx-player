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
var __spreadArray = (this && this.__spreadArray) || function (to, from, pack) {
    if (pack || arguments.length === 2) for (var i = 0, l = from.length, ar; i < l; i++) {
        if (ar || !(i in from)) {
            if (!ar) ar = Array.prototype.slice.call(from, 0, i);
            ar[i] = from[i];
        }
    }
    return to.concat(ar || Array.prototype.slice.call(from));
};
/**
 * This file defines a global clock for the RxPlayer.
 *
 * Each clock tick also pass information about the current state of the
 * media element to sub-parts of the player.
 */
import { defer as observableDefer, fromEvent as observableFromEvent, interval as observableInterval, merge as observableMerge, } from "rxjs";
import { map, mapTo, shareReplay, startWith, } from "rxjs/operators";
import config from "../../config";
import log from "../../log";
import objectAssign from "../../utils/object_assign";
import { getRange } from "../../utils/ranges";
var SAMPLING_INTERVAL_MEDIASOURCE = config.SAMPLING_INTERVAL_MEDIASOURCE, SAMPLING_INTERVAL_LOW_LATENCY = config.SAMPLING_INTERVAL_LOW_LATENCY, SAMPLING_INTERVAL_NO_MEDIASOURCE = config.SAMPLING_INTERVAL_NO_MEDIASOURCE, RESUME_GAP_AFTER_SEEKING = config.RESUME_GAP_AFTER_SEEKING, RESUME_GAP_AFTER_NOT_ENOUGH_DATA = config.RESUME_GAP_AFTER_NOT_ENOUGH_DATA, RESUME_GAP_AFTER_BUFFERING = config.RESUME_GAP_AFTER_BUFFERING, REBUFFERING_GAP = config.REBUFFERING_GAP, MINIMUM_BUFFER_AMOUNT_BEFORE_FREEZING = config.MINIMUM_BUFFER_AMOUNT_BEFORE_FREEZING;
/**
 * HTMLMediaElement Events for which timings are calculated and emitted.
 * @type {Array.<string>}
 */
var SCANNED_MEDIA_ELEMENTS_EVENTS = ["canplay",
    "play",
    "seeking",
    "seeked",
    "loadedmetadata",
    "ratechange"];
/**
 * Returns the amount of time in seconds the buffer should have ahead of the
 * current position before resuming playback. Based on the infos of the
 * rebuffering status.
 *
 * Waiting time differs between a rebuffering happening after a "seek" or one
 * happening after a buffer starvation occured.
 * @param {Object|null} rebufferingStatus
 * @param {Boolean} lowLatencyMode
 * @returns {Number}
 */
function getRebufferingEndGap(rebufferingStatus, lowLatencyMode) {
    if (rebufferingStatus === null) {
        return 0;
    }
    var suffix = lowLatencyMode ? "LOW_LATENCY" :
        "DEFAULT";
    switch (rebufferingStatus.reason) {
        case "seeking":
            return RESUME_GAP_AFTER_SEEKING[suffix];
        case "not-ready":
            return RESUME_GAP_AFTER_NOT_ENOUGH_DATA[suffix];
        case "buffering":
            return RESUME_GAP_AFTER_BUFFERING[suffix];
    }
}
/**
 * @param {Object} currentRange
 * @param {Number} duration
 * @param {Boolean} lowLatencyMode
 * @returns {Boolean}
 */
function hasLoadedUntilTheEnd(currentRange, duration, lowLatencyMode) {
    var suffix = lowLatencyMode ? "LOW_LATENCY" :
        "DEFAULT";
    return currentRange !== null &&
        (duration - currentRange.end) <= REBUFFERING_GAP[suffix];
}
/**
 * Generate a basic timings object from the media element and the eventName
 * which triggered the request.
 * @param {HTMLMediaElement} mediaElement
 * @param {string} event
 * @returns {Object}
 */
function getMediaInfos(mediaElement, event) {
    var buffered = mediaElement.buffered, currentTime = mediaElement.currentTime, duration = mediaElement.duration, ended = mediaElement.ended, paused = mediaElement.paused, playbackRate = mediaElement.playbackRate, readyState = mediaElement.readyState, seeking = mediaElement.seeking;
    var currentRange = getRange(buffered, currentTime);
    return { bufferGap: currentRange !== null ? currentRange.end - currentTime :
            // TODO null/0 would probably be
            // more appropriate
            Infinity, buffered: buffered, currentRange: currentRange, position: currentTime, duration: duration, ended: ended, paused: paused, playbackRate: playbackRate, readyState: readyState, seeking: seeking, event: event };
}
/**
 * Infer rebuffering status of the media based on:
 *   - the return of the function getMediaInfos
 *   - the previous timings object.
 *
 * @param {Object} prevTimings - Previous timings object. See function to know
 * the different properties needed.
 * @param {Object} currentTimings - Current timings object. This does not need
 * to have every single infos, see function to know which properties are needed.
 * @param {Object} options
 * @returns {Object|null}
 */
function getRebufferingStatus(prevTimings, currentTimings, _a) {
    var withMediaSource = _a.withMediaSource, lowLatencyMode = _a.lowLatencyMode;
    var currentEvt = currentTimings.event, currentTime = currentTimings.position, bufferGap = currentTimings.bufferGap, currentRange = currentTimings.currentRange, duration = currentTimings.duration, paused = currentTimings.paused, readyState = currentTimings.readyState, ended = currentTimings.ended;
    var prevRebuffering = prevTimings.rebuffering, prevEvt = prevTimings.event, prevTime = prevTimings.position;
    var fullyLoaded = hasLoadedUntilTheEnd(currentRange, duration, lowLatencyMode);
    var canSwitchToRebuffering = (readyState >= 1 &&
        currentEvt !== "loadedmetadata" &&
        prevRebuffering === null &&
        !(fullyLoaded || ended));
    var rebufferEndPosition = null;
    var shouldRebuffer;
    var shouldStopRebuffer;
    var rebufferGap = lowLatencyMode ? REBUFFERING_GAP.LOW_LATENCY :
        REBUFFERING_GAP.DEFAULT;
    if (withMediaSource) {
        if (canSwitchToRebuffering) {
            if (bufferGap <= rebufferGap) {
                shouldRebuffer = true;
                rebufferEndPosition = currentTime + bufferGap;
            }
            else if (bufferGap === Infinity) {
                shouldRebuffer = true;
                rebufferEndPosition = currentTime;
            }
        }
        else if (prevRebuffering !== null) {
            var resumeGap = getRebufferingEndGap(prevRebuffering, lowLatencyMode);
            if (shouldRebuffer !== true && prevRebuffering !== null && readyState > 1 &&
                (fullyLoaded || ended || (bufferGap < Infinity && bufferGap > resumeGap))) {
                shouldStopRebuffer = true;
            }
            else if (bufferGap === Infinity || bufferGap <= resumeGap) {
                rebufferEndPosition = bufferGap === Infinity ? currentTime :
                    currentTime + bufferGap;
            }
        }
    }
    // when using a direct file, the media will stall and unstall on its
    // own, so we only try to detect when the media timestamp has not changed
    // between two consecutive timeupdates
    else {
        if (canSwitchToRebuffering &&
            (!paused && currentEvt === "timeupdate" &&
                prevEvt === "timeupdate" && currentTime === prevTime ||
                currentEvt === "seeking" && bufferGap === Infinity)) {
            shouldRebuffer = true;
        }
        else if (prevRebuffering !== null &&
            (currentEvt !== "seeking" && currentTime !== prevTime ||
                currentEvt === "canplay" ||
                bufferGap < Infinity &&
                    (bufferGap > getRebufferingEndGap(prevRebuffering, lowLatencyMode) ||
                        fullyLoaded || ended))) {
            shouldStopRebuffer = true;
        }
    }
    if (shouldStopRebuffer === true) {
        return null;
    }
    else if (shouldRebuffer === true || prevRebuffering !== null) {
        var reason = void 0;
        if (currentEvt === "seeking" ||
            prevRebuffering !== null && prevRebuffering.reason === "seeking") {
            reason = "seeking";
        }
        else if (currentTimings.seeking) {
            reason = "seeking";
        }
        else if (readyState === 1) {
            reason = "not-ready";
        }
        else {
            reason = "buffering";
        }
        if (prevRebuffering !== null && prevRebuffering.reason === reason) {
            return { reason: prevRebuffering.reason,
                timestamp: prevRebuffering.timestamp,
                position: rebufferEndPosition };
        }
        return { reason: reason, timestamp: performance.now(),
            position: rebufferEndPosition };
    }
    return null;
}
/**
 * Detect if the current media can be considered as "freezing" (i.e. not
 * advancing for unknown reasons).
 *
 * Returns a corresponding `IFreezingStatus` object if that's the case and
 * `null` if not.
 * @param {Object} prevTimings
 * @param {Object} currentTimings
 * @returns {Object|null}
 */
function getFreezingStatus(prevTimings, currentTimings) {
    if (prevTimings.freezing) {
        if (currentTimings.ended ||
            currentTimings.paused ||
            currentTimings.readyState === 0 ||
            currentTimings.playbackRate === 0 ||
            prevTimings.position !== currentTimings.position) {
            return null; // Quit freezing status
        }
        return prevTimings.freezing; // Stay in it
    }
    return currentTimings.event === "timeupdate" &&
        currentTimings.bufferGap > MINIMUM_BUFFER_AMOUNT_BEFORE_FREEZING &&
        !currentTimings.ended &&
        !currentTimings.paused &&
        currentTimings.readyState >= 1 &&
        currentTimings.playbackRate !== 0 &&
        currentTimings.position === prevTimings.position ?
        { timestamp: performance.now() } :
        null;
}
/**
 * Timings observable.
 *
 * This Observable samples snapshots of player's current state:
 *   * time position
 *   * playback rate
 *   * current buffered range
 *   * gap with current buffered range ending
 *   * media duration
 *
 * In addition to sampling, this Observable also reacts to "seeking" and "play"
 * events.
 *
 * Observable is shared for performance reason: reduces the number of event
 * listeners and intervals/timeouts but also limit access to the media element
 * properties and gap calculations.
 *
 * The sampling is manual instead of based on "timeupdate" to reduce the
 * number of events.
 * @param {HTMLMediaElement} mediaElement
 * @param {Object} options
 * @returns {Observable}
 */
function createClock(mediaElement, options) {
    // Allow us to identify seek performed internally by the player.
    var internalSeekingComingCounter = 0;
    function setCurrentTime(time) {
        mediaElement.currentTime = time;
        internalSeekingComingCounter += 1;
    }
    var clock$ = observableDefer(function () {
        var lastTimings = objectAssign(getMediaInfos(mediaElement, "init"), { rebuffering: null,
            freezing: null,
            internalSeeking: false,
            getCurrentTime: function () { return mediaElement.currentTime; } });
        function getCurrentClockTick(event) {
            var tmpEvt = event;
            if (tmpEvt === "seeking" && internalSeekingComingCounter > 0) {
                tmpEvt = "internal-seeking";
                internalSeekingComingCounter -= 1;
            }
            var mediaTimings = getMediaInfos(mediaElement, tmpEvt);
            var internalSeeking = mediaTimings.seeking &&
                // We've just received the event for internally seeking
                (tmpEvt === "internal-seeking" ||
                    // or We're still waiting on the previous internal-seek
                    (lastTimings.internalSeeking && tmpEvt !== "seeking"));
            var rebufferingStatus = getRebufferingStatus(lastTimings, mediaTimings, options);
            var freezingStatus = getFreezingStatus(lastTimings, mediaTimings);
            var timings = objectAssign({}, { rebuffering: rebufferingStatus,
                freezing: freezingStatus, internalSeeking: internalSeeking, getCurrentTime: function () { return mediaElement.currentTime; } }, mediaTimings);
            log.debug("API: current media element state", timings);
            return timings;
        }
        var eventObs = SCANNED_MEDIA_ELEMENTS_EVENTS.map(function (eventName) {
            return observableFromEvent(mediaElement, eventName)
                .pipe(mapTo(eventName));
        });
        var interval = options.lowLatencyMode ? SAMPLING_INTERVAL_LOW_LATENCY :
            options.withMediaSource ? SAMPLING_INTERVAL_MEDIASOURCE :
                SAMPLING_INTERVAL_NO_MEDIASOURCE;
        var interval$ = observableInterval(interval)
            .pipe(mapTo("timeupdate"));
        return observableMerge.apply(void 0, __spreadArray([interval$], eventObs, false)).pipe(map(function (event) {
            lastTimings = getCurrentClockTick(event);
            if (log.getLevel() === "DEBUG") {
                log.debug("API: current playback timeline:\n" +
                    prettyPrintBuffered(lastTimings.buffered, lastTimings.position), "\n" + event);
            }
            return lastTimings;
        }), startWith(lastTimings));
    }).pipe(
    // Always emit the last tick when already subscribed
    shareReplay({ bufferSize: 1, refCount: true }));
    return { clock$: clock$, setCurrentTime: setCurrentTime };
}
/**
 * Pretty print a TimeRanges Object, to see the current content of it in a
 * one-liner string.
 *
 * @example
 * This function is called by giving it directly the TimeRanges, such as:
 * ```js
 * prettyPrintBuffered(document.getElementsByTagName("video")[0].buffered);
 * ```
 *
 * Let's consider this possible return:
 *
 * ```
 * 0.00|==29.95==|29.95 ~30.05~ 60.00|==29.86==|89.86
 *          ^14
 * ```
 * This means that our video element has 29.95 seconds of buffer between 0 and
 * 29.95 seconds.
 * Then 30.05 seconds where no buffer is found.
 * Then 29.86 seconds of buffer between 60.00 and 89.86 seconds.
 *
 * A caret on the second line indicates the current time we're at.
 * The number coming after it is the current time.
 * @param {TimeRanges} buffered
 * @param {number} currentTime
 * @returns {string}
 */
function prettyPrintBuffered(buffered, currentTime) {
    var str = "";
    var currentTimeStr = "";
    for (var i = 0; i < buffered.length; i++) {
        var start = buffered.start(i);
        var end = buffered.end(i);
        var fixedStart = start.toFixed(2);
        var fixedEnd = end.toFixed(2);
        var fixedDuration = (end - start).toFixed(2);
        var newIntervalStr = fixedStart + "|==" + fixedDuration + "==|" + fixedEnd;
        str += newIntervalStr;
        if (currentTimeStr.length === 0 && end > currentTime) {
            var padBefore = str.length - Math.floor(newIntervalStr.length / 2);
            currentTimeStr = " ".repeat(padBefore) + ("^" + currentTime);
        }
        if (i < buffered.length - 1) {
            var nextStart = buffered.start(i + 1);
            var fixedDiff = (nextStart - end).toFixed(2);
            var holeStr = " ~" + fixedDiff + "~ ";
            str += holeStr;
            if (currentTimeStr.length === 0 && currentTime < nextStart) {
                var padBefore = str.length - Math.floor(holeStr.length / 2);
                currentTimeStr = " ".repeat(padBefore) + ("^" + currentTime);
            }
        }
    }
    if (currentTimeStr.length === 0) {
        currentTimeStr = " ".repeat(str.length) + ("^" + currentTime);
    }
    return str + "\n" + currentTimeStr;
}
export default createClock;