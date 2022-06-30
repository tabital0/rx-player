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
var __extends = (this && this.__extends) || (function () {
    var extendStatics = function (d, b) {
        extendStatics = Object.setPrototypeOf ||
            ({ __proto__: [] } instanceof Array && function (d, b) { d.__proto__ = b; }) ||
            function (d, b) { for (var p in b) if (Object.prototype.hasOwnProperty.call(b, p)) d[p] = b[p]; };
        return extendStatics(d, b);
    };
    return function (d, b) {
        if (typeof b !== "function" && b !== null)
            throw new TypeError("Class extends value " + String(b) + " is not a constructor or null");
        extendStatics(d, b);
        function __() { this.constructor = d; }
        d.prototype = b === null ? Object.create(b) : (__.prototype = b.prototype, new __());
    };
})();
var __assign = (this && this.__assign) || function () {
    __assign = Object.assign || function(t) {
        for (var s, i = 1, n = arguments.length; i < n; i++) {
            s = arguments[i];
            for (var p in s) if (Object.prototype.hasOwnProperty.call(s, p))
                t[p] = s[p];
        }
        return t;
    };
    return __assign.apply(this, arguments);
};
/**
 * This file defines the public API for the RxPlayer.
 * It also starts the different sub-parts of the player on various API calls.
 */
import { combineLatest as observableCombineLatest, concat as observableConcat, connectable, distinctUntilChanged, EMPTY, filter, map, merge as observableMerge, mergeMap, Observable, of as observableOf, share, shareReplay, skipWhile, startWith, Subject, switchMap, take, takeUntil, } from "rxjs";
import { events, exitFullscreen, getStartDate, isFullscreen, requestFullscreen, } from "../../compat";
/* eslint-disable-next-line max-len */
import canRelyOnVideoVisibilityAndSize from "../../compat/can_rely_on_video_visibility_and_size";
import config from "../../config";
import { ErrorCodes, ErrorTypes, formatError, MediaError, } from "../../errors";
import features from "../../features";
import log from "../../log";
import Manifest from "../../manifest";
import areArraysOfNumbersEqual from "../../utils/are_arrays_of_numbers_equal";
import EventEmitter from "../../utils/event_emitter";
import isNullOrUndefined from "../../utils/is_null_or_undefined";
import objectAssign from "../../utils/object_assign";
import { getLeftSizeOfRange, getPlayedSizeOfRange, getSizeOfRange, } from "../../utils/ranges";
import createSharedReference, { createMappedReference, } from "../../utils/reference";
import TaskCanceller from "../../utils/task_canceller";
import warnOnce from "../../utils/warn_once";
import { clearOnStop, disposeDecryptionResources, getCurrentKeySystem, } from "../decrypt";
import { ManifestFetcher, SegmentFetcherCreator, } from "../fetchers";
import initializeMediaSourcePlayback from "../init";
import emitSeekEvents from "./emit_seek_events";
import getPlayerState, { PLAYER_STATES, } from "./get_player_state";
import { checkReloadOptions, parseConstructorOptions, parseLoadVideoOptions, } from "./option_utils";
import PlaybackObserver from "./playback_observer";
import TrackChoiceManager from "./track_choice_manager";
/* eslint-disable @typescript-eslint/naming-convention */
var getPageActivityRef = events.getPageActivityRef, getPictureOnPictureStateRef = events.getPictureOnPictureStateRef, getVideoVisibilityRef = events.getVideoVisibilityRef, getVideoWidthRef = events.getVideoWidthRef, onFullscreenChange$ = events.onFullscreenChange$, onTextTrackChanges$ = events.onTextTrackChanges$;
/**
 * @class Player
 * @extends EventEmitter
 */
var Player = /** @class */ (function (_super) {
    __extends(Player, _super);
    /**
     * @constructor
     * @param {Object} options
     */
    function Player(options) {
        if (options === void 0) { options = {}; }
        var _this = _super.call(this) || this;
        var _a = parseConstructorOptions(options), initialAudioBitrate = _a.initialAudioBitrate, initialVideoBitrate = _a.initialVideoBitrate, limitVideoWidth = _a.limitVideoWidth, minAudioBitrate = _a.minAudioBitrate, minVideoBitrate = _a.minVideoBitrate, maxAudioBitrate = _a.maxAudioBitrate, maxBufferAhead = _a.maxBufferAhead, maxBufferBehind = _a.maxBufferBehind, maxVideoBitrate = _a.maxVideoBitrate, preferredAudioTracks = _a.preferredAudioTracks, preferredTextTracks = _a.preferredTextTracks, preferredVideoTracks = _a.preferredVideoTracks, throttleWhenHidden = _a.throttleWhenHidden, throttleVideoBitrateWhenHidden = _a.throttleVideoBitrateWhenHidden, videoElement = _a.videoElement, wantedBufferAhead = _a.wantedBufferAhead, maxVideoBufferSize = _a.maxVideoBufferSize, stopAtEnd = _a.stopAtEnd;
        var DEFAULT_UNMUTED_VOLUME = config.getCurrent().DEFAULT_UNMUTED_VOLUME;
        // Workaround to support Firefox autoplay on FF 42.
        // See: https://bugzilla.mozilla.org/show_bug.cgi?id=1194624
        videoElement.preload = "auto";
        _this.version = /* PLAYER_VERSION */ "3.28.0-samsung.seek3";
        _this.log = log;
        _this.state = "STOPPED";
        _this.videoElement = videoElement;
        var destroyCanceller = new TaskCanceller();
        _this._priv_destroy$ = new Subject(); // TODO Remove the need for this Subject
        _this._priv_destroy$.pipe(take(1)).subscribe(function () {
            destroyCanceller.cancel();
        });
        _this._priv_pictureInPictureRef = getPictureOnPictureStateRef(videoElement, destroyCanceller.signal);
        /** @deprecated */
        onFullscreenChange$(videoElement)
            .pipe(takeUntil(_this._priv_destroy$))
            /* eslint-disable import/no-deprecated */
            .subscribe(function () { return _this.trigger("fullscreenChange", _this.isFullscreen()); });
        /* eslint-enable import/no-deprecated */
        /** @deprecated */
        onTextTrackChanges$(videoElement.textTracks)
            .pipe(takeUntil(_this._priv_destroy$), map(function (evt) {
            var target = evt.target;
            var arr = [];
            for (var i = 0; i < target.length; i++) {
                var textTrack = target[i];
                arr.push(textTrack);
            }
            return arr;
        }), 
        // We can have two consecutive textTrackChanges with the exact same
        // payload when we perform multiple texttrack operations before the event
        // loop is freed.
        // In that case we only want to fire one time the observable.
        distinctUntilChanged(function (textTracksA, textTracksB) {
            if (textTracksA.length !== textTracksB.length) {
                return false;
            }
            for (var i = 0; i < textTracksA.length; i++) {
                if (textTracksA[i] !== textTracksB[i]) {
                    return false;
                }
            }
            return true;
        }))
            .subscribe(function (x) { return _this._priv_onNativeTextTracksNext(x); });
        _this._priv_speed = createSharedReference(videoElement.playbackRate);
        _this._priv_preferTrickModeTracks = false;
        _this._priv_contentLock = createSharedReference(false);
        _this._priv_bufferOptions = {
            wantedBufferAhead: createSharedReference(wantedBufferAhead),
            maxBufferAhead: createSharedReference(maxBufferAhead),
            maxBufferBehind: createSharedReference(maxBufferBehind),
            maxVideoBufferSize: createSharedReference(maxVideoBufferSize),
        };
        _this._priv_bitrateInfos = {
            lastBitrates: { audio: initialAudioBitrate,
                video: initialVideoBitrate },
            minAutoBitrates: { audio: createSharedReference(minAudioBitrate),
                video: createSharedReference(minVideoBitrate) },
            maxAutoBitrates: { audio: createSharedReference(maxAudioBitrate),
                video: createSharedReference(maxVideoBitrate) },
            manualBitrates: { audio: createSharedReference(-1),
                video: createSharedReference(-1) },
        };
        _this._priv_throttleWhenHidden = throttleWhenHidden;
        _this._priv_throttleVideoBitrateWhenHidden = throttleVideoBitrateWhenHidden;
        _this._priv_limitVideoWidth = limitVideoWidth;
        _this._priv_mutedMemory = DEFAULT_UNMUTED_VOLUME;
        _this._priv_trackChoiceManager = null;
        _this._priv_mediaElementTrackChoiceManager = null;
        _this._priv_currentError = null;
        _this._priv_contentInfos = null;
        _this._priv_contentEventsMemory = {};
        _this._priv_stopAtEnd = stopAtEnd;
        _this._priv_setPlayerState(PLAYER_STATES.STOPPED);
        _this._priv_preferredAudioTracks = preferredAudioTracks;
        _this._priv_preferredTextTracks = preferredTextTracks;
        _this._priv_preferredVideoTracks = preferredVideoTracks;
        _this._priv_lastContentPlaybackInfos = {};
        return _this;
    }
    Object.defineProperty(Player, "ErrorTypes", {
        /** All possible Error types emitted by the RxPlayer. */
        get: function () {
            return ErrorTypes;
        },
        enumerable: false,
        configurable: true
    });
    Object.defineProperty(Player, "ErrorCodes", {
        /** All possible Error codes emitted by the RxPlayer. */
        get: function () {
            return ErrorCodes;
        },
        enumerable: false,
        configurable: true
    });
    Object.defineProperty(Player, "LogLevel", {
        /**
         * Current log level.
         * Update current log level.
         * Should be either (by verbosity ascending):
         *   - "NONE"
         *   - "ERROR"
         *   - "WARNING"
         *   - "INFO"
         *   - "DEBUG"
         * Any other value will be translated to "NONE".
         */
        get: function () {
            return log.getLevel();
        },
        set: function (logLevel) {
            log.setLevel(logLevel);
        },
        enumerable: false,
        configurable: true
    });
    /**
     * Register a new callback for a player event event.
     *
     * @param {string} evt - The event to register a callback to
     * @param {Function} fn - The callback to call as that event is triggered.
     * The callback will take as argument the eventual payload of the event
     * (single argument).
     */
    Player.prototype.addEventListener = function (evt, fn) {
        // The EventEmitter's `addEventListener` method takes an optional third
        // argument that we do not want to expose in the public API.
        // We thus overwrite that function to remove any possible usage of that
        // third argument.
        return _super.prototype.addEventListener.call(this, evt, fn);
    };
    /**
     * Stop the playback for the current content.
     */
    Player.prototype.stop = function () {
        if (this._priv_contentInfos !== null) {
            this._priv_contentInfos.currentContentCanceller.cancel();
        }
        this._priv_cleanUpCurrentContentState();
        if (this.state !== PLAYER_STATES.STOPPED) {
            this._priv_setPlayerState(PLAYER_STATES.STOPPED);
        }
    };
    /**
     * Free the resources used by the player.
     * /!\ The player cannot be "used" anymore after this method has been called.
     */
    Player.prototype.dispose = function () {
        // free resources linked to the loaded content
        this.stop();
        if (this.videoElement !== null) {
            // free resources used for decryption management
            disposeDecryptionResources(this.videoElement)
                .catch(function (err) {
                var message = err instanceof Error ? err.message :
                    "Unknown error";
                log.error("API: Could not dispose decryption resources: " + message);
            });
        }
        // free Observables linked to the Player instance
        this._priv_destroy$.next();
        this._priv_destroy$.complete();
        // Complete all subjects and references
        this._priv_speed.finish();
        this._priv_contentLock.finish();
        this._priv_bufferOptions.wantedBufferAhead.finish();
        this._priv_bufferOptions.maxVideoBufferSize.finish();
        this._priv_bufferOptions.maxBufferAhead.finish();
        this._priv_bufferOptions.maxBufferBehind.finish();
        this._priv_bitrateInfos.manualBitrates.video.finish();
        this._priv_bitrateInfos.manualBitrates.audio.finish();
        this._priv_bitrateInfos.minAutoBitrates.video.finish();
        this._priv_bitrateInfos.minAutoBitrates.audio.finish();
        this._priv_bitrateInfos.maxAutoBitrates.video.finish();
        this._priv_bitrateInfos.maxAutoBitrates.audio.finish();
        this._priv_lastContentPlaybackInfos = {};
        // un-attach video element
        this.videoElement = null;
    };
    /**
     * Load a new video.
     * @param {Object} opts
     */
    Player.prototype.loadVideo = function (opts) {
        var options = parseLoadVideoOptions(opts);
        log.info("API: Calling loadvideo", options.url, options.transport);
        this._priv_lastContentPlaybackInfos = { options: options };
        this._priv_initializeContentPlayback(options);
    };
    /**
     * Reload last content. Init media playback without fetching again
     * the manifest.
     * @param {Object} reloadOpts
     */
    Player.prototype.reload = function (reloadOpts) {
        var _a = this._priv_lastContentPlaybackInfos, options = _a.options, manifest = _a.manifest, lastPlaybackPosition = _a.lastPlaybackPosition;
        if (options === undefined ||
            manifest === undefined ||
            lastPlaybackPosition === undefined) {
            throw new Error("API: Can't reload without having previously loaded a content.");
        }
        checkReloadOptions(reloadOpts);
        var startAtPositon;
        if (reloadOpts !== undefined &&
            reloadOpts.reloadAt !== undefined &&
            reloadOpts.reloadAt.position !== undefined) {
            startAtPositon = reloadOpts.reloadAt.position;
        }
        else {
            var playbackPosition = void 0;
            if (this.state === "STOPPED" || this.state === "ENDED") {
                playbackPosition = lastPlaybackPosition;
            }
            else {
                if (this.videoElement === null) {
                    throw new Error("Can't reload when video element does not exist.");
                }
                playbackPosition = this.videoElement.currentTime;
            }
            if (reloadOpts !== undefined &&
                reloadOpts.reloadAt !== undefined &&
                reloadOpts.reloadAt.relative !== undefined) {
                startAtPositon = reloadOpts.reloadAt.relative + playbackPosition;
            }
            else {
                startAtPositon = playbackPosition;
            }
        }
        var newOptions = __assign(__assign({}, options), { initialManifest: manifest });
        newOptions.startAt = { position: startAtPositon };
        this._priv_initializeContentPlayback(newOptions);
    };
    /**
     * From given options, initialize content playback.
     * @param {Object} options
     */
    Player.prototype._priv_initializeContentPlayback = function (options) {
        var _this = this;
        var _a, _b, _c;
        var autoPlay = options.autoPlay, audioTrackSwitchingMode = options.audioTrackSwitchingMode, defaultAudioTrack = options.defaultAudioTrack, defaultTextTrack = options.defaultTextTrack, enableFastSwitching = options.enableFastSwitching, initialManifest = options.initialManifest, keySystems = options.keySystems, lowLatencyMode = options.lowLatencyMode, manualBitrateSwitchingMode = options.manualBitrateSwitchingMode, minimumManifestUpdateInterval = options.minimumManifestUpdateInterval, networkConfig = options.networkConfig, onCodecSwitch = options.onCodecSwitch, startAt = options.startAt, transport = options.transport, transportOptions = options.transportOptions, url = options.url;
        // Perform multiple checks on the given options
        if (this.videoElement === null) {
            throw new Error("the attached video element is disposed");
        }
        var isDirectFile = transport === "directfile";
        /** Subject which will emit to stop the current content. */
        var currentContentCanceller = new TaskCanceller();
        // Some logic needs the equivalent of the `currentContentCanceller` under
        // an Observable form
        // TODO remove the need for `stoppedContent$`
        var stoppedContent$ = new Observable(function (obs) {
            currentContentCanceller.signal.register(function () {
                obs.next();
                obs.complete();
            });
        });
        /** Future `this._priv_contentInfos` related to this content. */
        var contentInfos = { url: url, currentContentCanceller: currentContentCanceller, isDirectFile: isDirectFile, segmentBuffersStore: null,
            thumbnails: null,
            manifest: null,
            currentPeriod: null,
            activeAdaptations: null,
            activeRepresentations: null,
            initialAudioTrack: defaultAudioTrack,
            initialTextTrack: defaultTextTrack };
        var videoElement = this.videoElement;
        /** Global "playback observer" which will emit playback conditions */
        var playbackObserver = new PlaybackObserver(videoElement, {
            withMediaSource: !isDirectFile,
            lowLatencyMode: lowLatencyMode,
        });
        currentContentCanceller.signal.register(function () {
            playbackObserver.stop();
        });
        /** Emit playback events. */
        var playback$;
        if (!isDirectFile) {
            var transportFn = features.transports[transport];
            if (typeof transportFn !== "function") {
                // Stop previous content and reset its state
                this.stop();
                this._priv_currentError = null;
                throw new Error("transport \"".concat(transport, "\" not supported"));
            }
            var transportPipelines = transportFn(transportOptions);
            var offlineRetry = networkConfig.offlineRetry, segmentRetry = networkConfig.segmentRetry, manifestRetry = networkConfig.manifestRetry;
            /** Interface used to load and refresh the Manifest. */
            var manifestFetcher = new ManifestFetcher(url, transportPipelines, { lowLatencyMode: lowLatencyMode, maxRetryRegular: manifestRetry,
                maxRetryOffline: offlineRetry });
            /** Interface used to download segments. */
            var segmentFetcherCreator = new SegmentFetcherCreator(transportPipelines, { lowLatencyMode: lowLatencyMode, maxRetryOffline: offlineRetry,
                maxRetryRegular: segmentRetry });
            /** Observable emitting the initial Manifest */
            var manifest$ = void 0;
            if (initialManifest instanceof Manifest) {
                manifest$ = observableOf({ type: "parsed",
                    manifest: initialManifest });
            }
            else if (initialManifest !== undefined) {
                manifest$ = manifestFetcher.parse(initialManifest, { previousManifest: null,
                    unsafeMode: false });
            }
            else {
                manifest$ = manifestFetcher.fetch(url).pipe(mergeMap(function (response) { return response.type === "warning" ?
                    observableOf(response) : // bubble-up warnings
                    response.parse({ previousManifest: null, unsafeMode: false }); }));
            }
            // Load the Manifest right now and share it with every subscriber until
            // the content is stopped
            manifest$ = manifest$.pipe(takeUntil(stoppedContent$), shareReplay());
            manifest$.subscribe();
            // now that the Manifest is loading, stop previous content and reset state
            // This is done after fetching the Manifest as `stop` could technically
            // take time.
            this.stop();
            this._priv_currentError = null;
            this._priv_contentInfos = contentInfos;
            var relyOnVideoVisibilityAndSize = canRelyOnVideoVisibilityAndSize();
            var throttlers = { throttle: {},
                throttleBitrate: {},
                limitWidth: {} };
            if (this._priv_throttleWhenHidden) {
                if (!relyOnVideoVisibilityAndSize) {
                    log.warn("API: Can't apply throttleWhenHidden because " +
                        "browser can't be trusted for visibility.");
                }
                else {
                    throttlers.throttle = {
                        video: createMappedReference(getPageActivityRef(currentContentCanceller.signal), function (isActive) { return isActive ? Infinity : 0; }, currentContentCanceller.signal),
                    };
                }
            }
            if (this._priv_throttleVideoBitrateWhenHidden) {
                if (!relyOnVideoVisibilityAndSize) {
                    log.warn("API: Can't apply throttleVideoBitrateWhenHidden because " +
                        "browser can't be trusted for visibility.");
                }
                else {
                    throttlers.throttleBitrate = {
                        video: createMappedReference(getVideoVisibilityRef(this._priv_pictureInPictureRef, currentContentCanceller.signal), function (isActive) { return isActive ? Infinity : 0; }, currentContentCanceller.signal),
                    };
                }
            }
            if (this._priv_limitVideoWidth) {
                if (!relyOnVideoVisibilityAndSize) {
                    log.warn("API: Can't apply limitVideoWidth because browser can't be " +
                        "trusted for video size.");
                }
                else {
                    throttlers.limitWidth = {
                        video: getVideoWidthRef(videoElement, this._priv_pictureInPictureRef, currentContentCanceller.signal),
                    };
                }
            }
            /** Options used by the adaptive logic. */
            var adaptiveOptions = {
                initialBitrates: this._priv_bitrateInfos.lastBitrates,
                lowLatencyMode: lowLatencyMode,
                manualBitrates: this._priv_bitrateInfos.manualBitrates,
                minAutoBitrates: this._priv_bitrateInfos.minAutoBitrates,
                maxAutoBitrates: this._priv_bitrateInfos.maxAutoBitrates,
                throttlers: throttlers,
            };
            /** Options used by the TextTrack SegmentBuffer. */
            var textTrackOptions = options.textTrackMode === "native" ?
                { textTrackMode: "native",
                    hideNativeSubtitle: options.hideNativeSubtitle } :
                { textTrackMode: "html",
                    textTrackElement: options.textTrackElement };
            var bufferOptions = objectAssign({ audioTrackSwitchingMode: audioTrackSwitchingMode, enableFastSwitching: enableFastSwitching, manualBitrateSwitchingMode: manualBitrateSwitchingMode, onCodecSwitch: onCodecSwitch }, this._priv_bufferOptions);
            // We've every options set up. Start everything now
            var init$ = initializeMediaSourcePlayback({ adaptiveOptions: adaptiveOptions, autoPlay: autoPlay, bufferOptions: bufferOptions, playbackObserver: playbackObserver, keySystems: keySystems, lowLatencyMode: lowLatencyMode, manifest$: manifest$, manifestFetcher: manifestFetcher, mediaElement: videoElement, minimumManifestUpdateInterval: minimumManifestUpdateInterval, segmentFetcherCreator: segmentFetcherCreator, speed: this._priv_speed, startAt: startAt, textTrackOptions: textTrackOptions })
                .pipe(takeUntil(stoppedContent$));
            playback$ = connectable(init$, { connector: function () { return new Subject(); },
                resetOnDisconnect: false });
        }
        else {
            // Stop previous content and reset its state
            this.stop();
            this._priv_currentError = null;
            if (features.directfile === null) {
                throw new Error("DirectFile feature not activated in your build.");
            }
            this._priv_contentInfos = contentInfos;
            this._priv_mediaElementTrackChoiceManager =
                new features.directfile.mediaElementTrackChoiceManager(this.videoElement);
            var preferredAudioTracks = defaultAudioTrack === undefined ?
                this._priv_preferredAudioTracks :
                [defaultAudioTrack];
            this._priv_mediaElementTrackChoiceManager
                .setPreferredAudioTracks(preferredAudioTracks, true);
            var preferredTextTracks = defaultTextTrack === undefined ?
                this._priv_preferredTextTracks :
                [defaultTextTrack];
            this._priv_mediaElementTrackChoiceManager
                .setPreferredTextTracks(preferredTextTracks, true);
            this._priv_mediaElementTrackChoiceManager
                .setPreferredVideoTracks(this._priv_preferredVideoTracks, true);
            this.trigger("availableAudioTracksChange", this._priv_mediaElementTrackChoiceManager.getAvailableAudioTracks());
            this.trigger("availableVideoTracksChange", this._priv_mediaElementTrackChoiceManager.getAvailableVideoTracks());
            this.trigger("availableTextTracksChange", this._priv_mediaElementTrackChoiceManager.getAvailableTextTracks());
            this.trigger("audioTrackChange", (_a = this._priv_mediaElementTrackChoiceManager.getChosenAudioTrack()) !== null && _a !== void 0 ? _a : null);
            this.trigger("textTrackChange", (_b = this._priv_mediaElementTrackChoiceManager.getChosenTextTrack()) !== null && _b !== void 0 ? _b : null);
            this.trigger("videoTrackChange", (_c = this._priv_mediaElementTrackChoiceManager.getChosenVideoTrack()) !== null && _c !== void 0 ? _c : null);
            this._priv_mediaElementTrackChoiceManager
                .addEventListener("availableVideoTracksChange", function (val) {
                return _this.trigger("availableVideoTracksChange", val);
            });
            this._priv_mediaElementTrackChoiceManager
                .addEventListener("availableAudioTracksChange", function (val) {
                return _this.trigger("availableAudioTracksChange", val);
            });
            this._priv_mediaElementTrackChoiceManager
                .addEventListener("availableTextTracksChange", function (val) {
                return _this.trigger("availableTextTracksChange", val);
            });
            this._priv_mediaElementTrackChoiceManager
                .addEventListener("audioTrackChange", function (val) {
                return _this.trigger("audioTrackChange", val);
            });
            this._priv_mediaElementTrackChoiceManager
                .addEventListener("videoTrackChange", function (val) {
                return _this.trigger("videoTrackChange", val);
            });
            this._priv_mediaElementTrackChoiceManager
                .addEventListener("textTrackChange", function (val) {
                return _this.trigger("textTrackChange", val);
            });
            var directfileInit$ = features.directfile.initDirectFile({ autoPlay: autoPlay, keySystems: keySystems, mediaElement: videoElement,
                speed: this._priv_speed, playbackObserver: playbackObserver, startAt: startAt, url: url })
                .pipe(takeUntil(stoppedContent$));
            playback$ = connectable(directfileInit$, { connector: function () { return new Subject(); },
                resetOnDisconnect: false });
        }
        /** Emit an object when the player "stalls" and null when it un-stalls */
        var stalled$ = playback$.pipe(filter(function (evt) { return evt.type === "stalled" ||
            evt.type === "unstalled"; }), map(function (x) { return x.value; }), distinctUntilChanged(function (prevStallReason, currStallReason) {
            return prevStallReason === null && currStallReason === null ||
                (prevStallReason !== null && currStallReason !== null &&
                    prevStallReason === currStallReason);
        }));
        /** Emit when the content is considered "loaded". */
        var loaded$ = playback$.pipe(filter(function (evt) { return evt.type === "loaded"; }), share());
        /** Emit when we will "reload" the MediaSource. */
        var reloading$ = playback$
            .pipe(filter(function (evt) {
            return evt.type === "reloading-media-source";
        }), share());
        /** Emit when the media element emits a "seeking" event. */
        var observation$ = playbackObserver.getReference().asObservable();
        var stateChangingEvent$ = observation$.pipe(filter(function (o) {
            return o.event === "seeking" || o.event === "ended" ||
                o.event === "play" || o.event === "pause";
        }));
        /** Emit state updates once the content is considered "loaded". */
        var loadedStateUpdates$ = observableCombineLatest([
            stalled$.pipe(startWith(null)),
            stateChangingEvent$.pipe(startWith(null)),
        ]).pipe(takeUntil(stoppedContent$), map(function (_a) {
            var stalledStatus = _a[0];
            return getPlayerState(videoElement, stalledStatus);
        }));
        /** Emit all player "state" updates. */
        var playerState$ = observableConcat(observableOf(PLAYER_STATES.LOADING), // Begin with LOADING
        loaded$.pipe(switchMap(function (_, i) {
            var isFirstLoad = i === 0;
            return observableMerge(
            // Purposely subscribed first so a RELOADING triggered synchronously
            // after a LOADED state is catched.
            reloading$.pipe(map(function () { return PLAYER_STATES.RELOADING; })), 
            // Only switch to LOADED state for the first (i.e. non-RELOADING) load
            isFirstLoad ? observableOf(PLAYER_STATES.LOADED) :
                EMPTY, 
            // Purposely put last so any other state change happens after we've
            // already switched to LOADED
            loadedStateUpdates$.pipe(takeUntil(reloading$), 
            // For the first load, we prefer staying at the LOADED state over
            // PAUSED when autoPlay is disabled.
            // For consecutive loads however, there's no LOADED state.
            skipWhile(function (state) { return isFirstLoad && state === PLAYER_STATES.PAUSED; })));
        }))).pipe(distinctUntilChanged());
        var playbackSubscription;
        stoppedContent$.subscribe(function () {
            if (playbackSubscription !== undefined) {
                playbackSubscription.unsubscribe();
            }
        });
        // Link "positionUpdate" events to the PlaybackObserver
        observation$
            .pipe(takeUntil(stoppedContent$))
            .subscribe(function (o) { return _this._priv_triggerPositionUpdate(o); });
        // Link "seeking" and "seeked" events (once the content is loaded)
        loaded$.pipe(switchMap(function () { return emitSeekEvents(_this.videoElement, observation$); }), takeUntil(stoppedContent$)).subscribe(function (evt) {
            log.info("API: Triggering \"".concat(evt, "\" event"));
            _this.trigger(evt, null);
        });
        // Handle state updates
        playerState$
            .pipe(takeUntil(stoppedContent$))
            .subscribe(function (x) { return _this._priv_setPlayerState(x); });
        var endedEvent$ = observation$.pipe(filter(function (o) {
            return o.event === "ended";
        }));
        (this._priv_stopAtEnd ? endedEvent$ :
            EMPTY)
            .pipe(takeUntil(stoppedContent$))
            .subscribe(function () {
            currentContentCanceller.cancel();
        });
        // Link playback events to the corresponding callbacks
        playback$.subscribe({
            next: function (x) { return _this._priv_onPlaybackEvent(x); },
            error: function (err) { return _this._priv_onPlaybackError(err); },
            complete: function () {
                if (!contentInfos.currentContentCanceller.isUsed) {
                    log.info("API: Previous playback finished. Stopping and cleaning-up...");
                    contentInfos.currentContentCanceller.cancel();
                    _this._priv_cleanUpCurrentContentState();
                    _this._priv_setPlayerState(PLAYER_STATES.STOPPED);
                }
            },
        });
        // initialize the content only when the lock is inactive
        this._priv_contentLock.asObservable()
            .pipe(filter(function (isLocked) { return !isLocked; }), take(1), takeUntil(stoppedContent$))
            .subscribe(function () {
            // start playback!
            playbackSubscription = playback$.connect();
        });
    };
    /**
     * Returns fatal error if one for the current content.
     * null otherwise.
     * @returns {Object|null} - The current Error (`null` when no error).
     */
    Player.prototype.getError = function () {
        return this._priv_currentError;
    };
    /**
     * Returns manifest/playlist object.
     * null if the player is STOPPED.
     * @deprecated
     * @returns {Manifest|null} - The current Manifest (`null` when not known).
     */
    Player.prototype.getManifest = function () {
        warnOnce("getManifest is deprecated." +
            " Please open an issue if you used this API.");
        if (this._priv_contentInfos === null) {
            return null;
        }
        return this._priv_contentInfos.manifest;
    };
    /**
     * Returns Adaptations (tracks) for every currently playing type
     * (audio/video/text...).
     * @deprecated
     * @returns {Object|null} - The current Adaptation objects, per type (`null`
     * when none is known for now.
     */
    Player.prototype.getCurrentAdaptations = function () {
        warnOnce("getCurrentAdaptations is deprecated." +
            " Please open an issue if you used this API.");
        if (this._priv_contentInfos === null) {
            return null;
        }
        var _a = this._priv_contentInfos, currentPeriod = _a.currentPeriod, activeAdaptations = _a.activeAdaptations;
        if (currentPeriod === null ||
            activeAdaptations === null ||
            isNullOrUndefined(activeAdaptations[currentPeriod.id])) {
            return null;
        }
        return activeAdaptations[currentPeriod.id];
    };
    /**
     * Returns representations (qualities) for every currently playing type
     * (audio/video/text...).
     * @deprecated
     * @returns {Object|null} - The current Representation objects, per type
     * (`null` when none is known for now.
     */
    Player.prototype.getCurrentRepresentations = function () {
        warnOnce("getCurrentRepresentations is deprecated." +
            " Please open an issue if you used this API.");
        return this._priv_getCurrentRepresentations();
    };
    /**
     * Returns the media DOM element used by the player.
     * You should not its HTML5 API directly and use the player's method instead,
     * to ensure a well-behaved player.
     * @returns {HTMLMediaElement|null} - The HTMLMediaElement used (`null` when
     * disposed)
     */
    Player.prototype.getVideoElement = function () {
        return this.videoElement;
    };
    /**
     * If one returns the first native text-track element attached to the media element.
     * @deprecated
     * @returns {TextTrack} - The native TextTrack attached (`null` when none)
     */
    Player.prototype.getNativeTextTrack = function () {
        warnOnce("getNativeTextTrack is deprecated." +
            " Please open an issue if you used this API.");
        if (this.videoElement === null) {
            throw new Error("Disposed player");
        }
        var videoElement = this.videoElement;
        var textTracks = videoElement.textTracks;
        if (textTracks.length > 0) {
            return videoElement.textTracks[0];
        }
        else {
            return null;
        }
    };
    /**
     * Returns the player's current state.
     * @returns {string} - The current Player's state
     */
    Player.prototype.getPlayerState = function () {
        return this.state;
    };
    /**
     * Returns true if both:
     *   - a content is loaded
     *   - the content loaded is a live content
     * @returns {Boolean} - `true` if we're playing a live content, `false` otherwise.
     */
    Player.prototype.isLive = function () {
        if (this._priv_contentInfos === null) {
            return false;
        }
        var _a = this._priv_contentInfos, isDirectFile = _a.isDirectFile, manifest = _a.manifest;
        if (isDirectFile || manifest === null) {
            return false;
        }
        return manifest.isLive;
    };
    /**
     * Returns `true` if trickmode playback is active (usually through the usage
     * of the `setPlaybackRate` method), which means that the RxPlayer selects
     * "trickmode" video tracks in priority.
     * @returns {Boolean}
     */
    Player.prototype.areTrickModeTracksEnabled = function () {
        return this._priv_preferTrickModeTracks;
    };
    /**
     * Returns the url of the content's manifest
     * @returns {string|undefined} - Current URL. `undefined` if not known or no
     * URL yet.
     */
    Player.prototype.getUrl = function () {
        if (this._priv_contentInfos === null) {
            return undefined;
        }
        var _a = this._priv_contentInfos, isDirectFile = _a.isDirectFile, manifest = _a.manifest, url = _a.url;
        if (isDirectFile) {
            return url;
        }
        if (manifest !== null) {
            return manifest.getUrl();
        }
        return undefined;
    };
    /**
     * Returns the video duration, in seconds.
     * NaN if no video is playing.
     * @returns {Number}
     */
    Player.prototype.getVideoDuration = function () {
        if (this.videoElement === null) {
            throw new Error("Disposed player");
        }
        return this.videoElement.duration;
    };
    /**
     * Returns in seconds the difference between:
     *   - the end of the current contiguous loaded range.
     *   - the current time
     * @returns {Number}
     */
    Player.prototype.getVideoBufferGap = function () {
        if (this.videoElement === null) {
            throw new Error("Disposed player");
        }
        var videoElement = this.videoElement;
        return getLeftSizeOfRange(videoElement.buffered, videoElement.currentTime);
    };
    /**
     * Returns in seconds the difference between:
     *   - the end of the current contiguous loaded range.
     *   - the start of the current contiguous loaded range.
     * @returns {Number}
     */
    Player.prototype.getVideoLoadedTime = function () {
        if (this.videoElement === null) {
            throw new Error("Disposed player");
        }
        var videoElement = this.videoElement;
        return getSizeOfRange(videoElement.buffered, videoElement.currentTime);
    };
    /**
     * Returns in seconds the difference between:
     *   - the current time.
     *   - the start of the current contiguous loaded range.
     * @returns {Number}
     */
    Player.prototype.getVideoPlayedTime = function () {
        if (this.videoElement === null) {
            throw new Error("Disposed player");
        }
        var videoElement = this.videoElement;
        return getPlayedSizeOfRange(videoElement.buffered, videoElement.currentTime);
    };
    /**
     * Get the current position, in s, in wall-clock time.
     * That is:
     *   - for live content, get a timestamp, in s, of the current played content.
     *   - for static content, returns the position from beginning in s.
     *
     * If you do not know if you want to use this method or getPosition:
     *   - If what you want is to display the current time to the user, use this
     *     one.
     *   - If what you want is to interact with the player's API or perform other
     *     actions (like statistics) with the real player data, use getPosition.
     *
     * @returns {Number}
     */
    Player.prototype.getWallClockTime = function () {
        if (this.videoElement === null) {
            throw new Error("Disposed player");
        }
        if (this._priv_contentInfos === null) {
            return this.videoElement.currentTime;
        }
        var _a = this._priv_contentInfos, isDirectFile = _a.isDirectFile, manifest = _a.manifest;
        if (isDirectFile) {
            var startDate = getStartDate(this.videoElement);
            return (startDate !== null && startDate !== void 0 ? startDate : 0) + this.videoElement.currentTime;
        }
        if (manifest !== null) {
            var currentTime = this.videoElement.currentTime;
            var ast = manifest.availabilityStartTime !== undefined ?
                manifest.availabilityStartTime :
                0;
            return currentTime + ast;
        }
        return 0;
    };
    /**
     * Get the current position, in seconds, of the video element.
     *
     * If you do not know if you want to use this method or getWallClockTime:
     *   - If what you want is to display the current time to the user, use
     *     getWallClockTime.
     *   - If what you want is to interact with the player's API or perform other
     *     actions (like statistics) with the real player data, use this one.
     *
     * @returns {Number}
     */
    Player.prototype.getPosition = function () {
        if (this.videoElement === null) {
            throw new Error("Disposed player");
        }
        return this.videoElement.currentTime;
    };
    /**
     * Returns the current playback rate at which the video plays.
     * @returns {Number}
     */
    Player.prototype.getPlaybackRate = function () {
        return this._priv_speed.getValue();
    };
    /**
     * Update the playback rate of the video.
     *
     * This method's effect is persisted from content to content, and can be
     * called even when no content is playing (it will still have an effect for
     * the next contents).
     *
     * If you want to reverse effects provoked by `setPlaybackRate` before playing
     * another content, you will have to call `setPlaybackRate` first with the
     * default settings you want to set.
     *
     * As an example, to reset the speed to "normal" (x1) speed and to disable
     * trickMode video tracks (which may have been enabled by a previous
     * `setPlaybackRate` call), you can call:
     * ```js
     * player.setPlaybackRate(1, { preferTrickModeTracks: false });
     * ```
     *
     * --
     *
     * This method can be used to switch to or exit from "trickMode" video tracks,
     * which are tracks specifically defined to mimic the visual aspect of a VCR's
     * fast forward/rewind feature, by only displaying a few video frames during
     * playback.
     *
     * This behavior is configurable through the second argument, by adding a
     * property named `preferTrickModeTracks` to that object.
     *
     * You can set that value to `true` to switch to trickMode video tracks when
     * available, and set it to `false` when you want to disable that logic.
     * Note that like any configuration given to `setPlaybackRate`, this setting
     * is persisted through all future contents played by the player.
     *
     * If you want to stop enabling trickMode tracks, you will have to call
     * `setPlaybackRate` again with `preferTrickModeTracks` set to `false`.
     *
     * You can know at any moment whether this behavior is enabled by calling
     * the `areTrickModeTracksEnabled` method. This will only means that the
     * RxPlayer will select in priority trickmode video tracks, not that the
     * currently chosen video tracks is a trickmode track (for example, some
     * contents may have no trickmode tracks available).
     *
     * If you want to know about the latter instead, you can call `getVideoTrack`
     * and/or listen to `videoTrackChange` events. The track returned may have an
     * `isTrickModeTrack` property set to `true`, indicating that it is a
     * trickmode track.
     *
     * Note that switching to or getting out of a trickmode video track may
     * lead to the player being a brief instant in a `"RELOADING"` state (notified
     * through `playerStateChange` events and the `getPlayerState` method). When in
     * that state, a black screen may be displayed and multiple RxPlayer APIs will
     * not be usable.
     *
     * @param {Number} rate
     * @param {Object} opts
     */
    Player.prototype.setPlaybackRate = function (rate, opts) {
        if (rate !== this._priv_speed.getValue()) {
            this._priv_speed.setValue(rate);
        }
        var preferTrickModeTracks = opts === null || opts === void 0 ? void 0 : opts.preferTrickModeTracks;
        if (typeof preferTrickModeTracks !== "boolean") {
            return;
        }
        this._priv_preferTrickModeTracks = preferTrickModeTracks;
        if (this._priv_trackChoiceManager !== null) {
            if (preferTrickModeTracks &&
                !this._priv_trackChoiceManager.isTrickModeEnabled()) {
                this._priv_trackChoiceManager.enableVideoTrickModeTracks();
            }
            else if (!preferTrickModeTracks &&
                this._priv_trackChoiceManager.isTrickModeEnabled()) {
                this._priv_trackChoiceManager.disableVideoTrickModeTracks();
            }
        }
    };
    /**
     * Returns all available bitrates for the current video Adaptation.
     * @returns {Array.<Number>}
     */
    Player.prototype.getAvailableVideoBitrates = function () {
        if (this._priv_contentInfos === null) {
            return [];
        }
        var _a = this._priv_contentInfos, currentPeriod = _a.currentPeriod, activeAdaptations = _a.activeAdaptations;
        if (currentPeriod === null || activeAdaptations === null) {
            return [];
        }
        var adaptations = activeAdaptations[currentPeriod.id];
        if (adaptations === undefined || isNullOrUndefined(adaptations.video)) {
            return [];
        }
        return adaptations.video.getAvailableBitrates();
    };
    /**
     * Returns all available bitrates for the current audio Adaptation.
     * @returns {Array.<Number>}
     */
    Player.prototype.getAvailableAudioBitrates = function () {
        if (this._priv_contentInfos === null) {
            return [];
        }
        var _a = this._priv_contentInfos, currentPeriod = _a.currentPeriod, activeAdaptations = _a.activeAdaptations;
        if (currentPeriod === null || activeAdaptations === null) {
            return [];
        }
        var adaptations = activeAdaptations[currentPeriod.id];
        if (adaptations === undefined || isNullOrUndefined(adaptations.audio)) {
            return [];
        }
        return adaptations.audio.getAvailableBitrates();
    };
    /**
     * Returns the manual audio bitrate set. -1 if in AUTO mode.
     * @returns {Number}
     */
    Player.prototype.getManualAudioBitrate = function () {
        return this._priv_bitrateInfos.manualBitrates.audio.getValue();
    };
    /**
     * Returns the manual video bitrate set. -1 if in AUTO mode.
     * @returns {Number}
     */
    Player.prototype.getManualVideoBitrate = function () {
        return this._priv_bitrateInfos.manualBitrates.video.getValue();
    };
    /**
     * Returns currently considered bitrate for video segments.
     * @returns {Number|undefined}
     */
    Player.prototype.getVideoBitrate = function () {
        var representations = this._priv_getCurrentRepresentations();
        if (representations === null || isNullOrUndefined(representations.video)) {
            return undefined;
        }
        return representations.video.bitrate;
    };
    /**
     * Returns currently considered bitrate for audio segments.
     * @returns {Number|undefined}
     */
    Player.prototype.getAudioBitrate = function () {
        var representations = this._priv_getCurrentRepresentations();
        if (representations === null || isNullOrUndefined(representations.audio)) {
            return undefined;
        }
        return representations.audio.bitrate;
    };
    /**
     * Returns minimum wanted video bitrate currently set.
     * @returns {Number}
     */
    Player.prototype.getMinVideoBitrate = function () {
        return this._priv_bitrateInfos.minAutoBitrates.video.getValue();
    };
    /**
     * Returns minimum wanted audio bitrate currently set.
     * @returns {Number}
     */
    Player.prototype.getMinAudioBitrate = function () {
        return this._priv_bitrateInfos.minAutoBitrates.audio.getValue();
    };
    /**
     * Returns maximum wanted video bitrate currently set.
     * @returns {Number}
     */
    Player.prototype.getMaxVideoBitrate = function () {
        return this._priv_bitrateInfos.maxAutoBitrates.video.getValue();
    };
    /**
     * Returns maximum wanted audio bitrate currently set.
     * @returns {Number}
     */
    Player.prototype.getMaxAudioBitrate = function () {
        return this._priv_bitrateInfos.maxAutoBitrates.audio.getValue();
    };
    /**
     * Play/Resume the current video.
     * @returns {Promise}
     */
    Player.prototype.play = function () {
        var _this = this;
        if (this.videoElement === null) {
            throw new Error("Disposed player");
        }
        var playPromise = this.videoElement.play();
        /* eslint-disable @typescript-eslint/unbound-method */
        if (isNullOrUndefined(playPromise) || typeof playPromise.catch !== "function") {
            /* eslint-enable @typescript-eslint/unbound-method */
            return Promise.resolve();
        }
        return playPromise.catch(function (error) {
            if (error.name === "NotAllowedError") {
                var warning = new MediaError("MEDIA_ERR_PLAY_NOT_ALLOWED", error.toString());
                _this.trigger("warning", warning);
            }
            throw error;
        });
    };
    /**
     * Pause the current video.
     */
    Player.prototype.pause = function () {
        if (this.videoElement === null) {
            throw new Error("Disposed player");
        }
        this.videoElement.pause();
    };
    /**
     * Seek to a given absolute position.
     * @param {Number|Object} time
     * @returns {Number} - The time the player has seek to
     */
    Player.prototype.seekTo = function (time) {
        var _a;
        if (this.videoElement === null) {
            throw new Error("Disposed player");
        }
        if (this._priv_contentInfos === null) {
            throw new Error("player: no content loaded");
        }
        var _b = this._priv_contentInfos, isDirectFile = _b.isDirectFile, manifest = _b.manifest;
        if (!isDirectFile && manifest === null) {
            throw new Error("player: the content did not load yet");
        }
        var positionWanted;
        if (typeof time === "number") {
            positionWanted = time;
        }
        else if (typeof time === "object") {
            var timeObj = time;
            var currentTs = this.videoElement.currentTime;
            if (!isNullOrUndefined(timeObj.relative)) {
                positionWanted = currentTs + timeObj.relative;
            }
            else if (!isNullOrUndefined(timeObj.position)) {
                positionWanted = timeObj.position;
            }
            else if (!isNullOrUndefined(timeObj.wallClockTime)) {
                if (manifest !== null) {
                    positionWanted = timeObj.wallClockTime - ((_a = manifest.availabilityStartTime) !== null && _a !== void 0 ? _a : 0);
                }
                else if (isDirectFile && this.videoElement !== null) {
                    var startDate = getStartDate(this.videoElement);
                    if (startDate !== undefined) {
                        positionWanted = timeObj.wallClockTime - startDate;
                    }
                }
                if (positionWanted === undefined) {
                    positionWanted = timeObj.wallClockTime;
                }
            }
            else {
                throw new Error("invalid time object. You must set one of the " +
                    "following properties: \"relative\", \"position\" or " +
                    "\"wallClockTime\"");
            }
        }
        if (positionWanted === undefined) {
            throw new Error("invalid time given");
        }
        this.videoElement.currentTime = positionWanted;
        return positionWanted;
    };
    /**
     * Returns true if the media element is full screen.
     * @deprecated
     * @returns {Boolean}
     */
    Player.prototype.isFullscreen = function () {
        warnOnce("isFullscreen is deprecated." +
            " Fullscreen management should now be managed by the application");
        return isFullscreen();
    };
    /**
     * Set/exit fullScreen.
     * @deprecated
     * @param {Boolean} [goFull=true] - if false, exit full screen.
     */
    Player.prototype.setFullscreen = function (goFull) {
        if (goFull === void 0) { goFull = true; }
        warnOnce("setFullscreen is deprecated." +
            " Fullscreen management should now be managed by the application");
        if (this.videoElement === null) {
            throw new Error("Disposed player");
        }
        if (goFull) {
            requestFullscreen(this.videoElement);
        }
        else {
            exitFullscreen();
        }
    };
    /**
     * Exit from full screen mode.
     * @deprecated
     */
    Player.prototype.exitFullscreen = function () {
        warnOnce("exitFullscreen is deprecated." +
            " Fullscreen management should now be managed by the application");
        exitFullscreen();
    };
    /**
     * Returns the current player's audio volume on the media element.
     * From 0 (no audio) to 1 (maximum volume).
     * @returns {Number}
     */
    Player.prototype.getVolume = function () {
        if (this.videoElement === null) {
            throw new Error("Disposed player");
        }
        return this.videoElement.volume;
    };
    /**
     * Set the player's audio volume. From 0 (no volume) to 1 (maximum volume).
     * @param {Number} volume
     */
    Player.prototype.setVolume = function (volume) {
        if (this.videoElement === null) {
            throw new Error("Disposed player");
        }
        var videoElement = this.videoElement;
        if (volume !== videoElement.volume) {
            videoElement.volume = volume;
            this.trigger("volumeChange", volume);
        }
    };
    /**
     * Returns true if the volume is set to 0. false otherwise.
     * @returns {Boolean}
     */
    Player.prototype.isMute = function () {
        return this.getVolume() === 0;
    };
    /**
     * Set the volume to 0 and save current one for when unmuted.
     */
    Player.prototype.mute = function () {
        this._priv_mutedMemory = this.getVolume();
        this.setVolume(0);
    };
    /**
     * Set the volume back to when it was when mute was last called.
     * If the volume was set to 0, set a default volume instead (see config).
     */
    Player.prototype.unMute = function () {
        var DEFAULT_UNMUTED_VOLUME = config.getCurrent().DEFAULT_UNMUTED_VOLUME;
        var vol = this.getVolume();
        if (vol === 0) {
            this.setVolume(this._priv_mutedMemory === 0 ? DEFAULT_UNMUTED_VOLUME :
                this._priv_mutedMemory);
        }
    };
    /**
     * Force the video bitrate to a given value. Act as a ceil.
     * -1 to set it on AUTO Mode
     * @param {Number} btr
     */
    Player.prototype.setVideoBitrate = function (btr) {
        this._priv_bitrateInfos.manualBitrates.video.setValue(btr);
    };
    /**
     * Force the audio bitrate to a given value. Act as a ceil.
     * -1 to set it on AUTO Mode
     * @param {Number} btr
     */
    Player.prototype.setAudioBitrate = function (btr) {
        this._priv_bitrateInfos.manualBitrates.audio.setValue(btr);
    };
    /**
     * Update the minimum video bitrate the user can switch to.
     * @param {Number} btr
     */
    Player.prototype.setMinVideoBitrate = function (btr) {
        var maxVideoBitrate = this._priv_bitrateInfos.maxAutoBitrates.video.getValue();
        if (btr > maxVideoBitrate) {
            throw new Error("Invalid minimum video bitrate given. " +
                "Its value, \"".concat(btr, "\" is superior the current maximum ") +
                "video birate, \"".concat(maxVideoBitrate, "\"."));
        }
        this._priv_bitrateInfos.minAutoBitrates.video.setValue(btr);
    };
    /**
     * Update the minimum audio bitrate the user can switch to.
     * @param {Number} btr
     */
    Player.prototype.setMinAudioBitrate = function (btr) {
        var maxAudioBitrate = this._priv_bitrateInfos.maxAutoBitrates.audio.getValue();
        if (btr > maxAudioBitrate) {
            throw new Error("Invalid minimum audio bitrate given. " +
                "Its value, \"".concat(btr, "\" is superior the current maximum ") +
                "audio birate, \"".concat(maxAudioBitrate, "\"."));
        }
        this._priv_bitrateInfos.minAutoBitrates.audio.setValue(btr);
    };
    /**
     * Update the maximum video bitrate the user can switch to.
     * @param {Number} btr
     */
    Player.prototype.setMaxVideoBitrate = function (btr) {
        var minVideoBitrate = this._priv_bitrateInfos.minAutoBitrates.video.getValue();
        if (btr < minVideoBitrate) {
            throw new Error("Invalid maximum video bitrate given. " +
                "Its value, \"".concat(btr, "\" is inferior the current minimum ") +
                "video birate, \"".concat(minVideoBitrate, "\"."));
        }
        this._priv_bitrateInfos.maxAutoBitrates.video.setValue(btr);
    };
    /**
     * Update the maximum audio bitrate the user can switch to.
     * @param {Number} btr
     */
    Player.prototype.setMaxAudioBitrate = function (btr) {
        var minAudioBitrate = this._priv_bitrateInfos.minAutoBitrates.audio.getValue();
        if (btr < minAudioBitrate) {
            throw new Error("Invalid maximum audio bitrate given. " +
                "Its value, \"".concat(btr, "\" is inferior the current minimum ") +
                "audio birate, \"".concat(minAudioBitrate, "\"."));
        }
        this._priv_bitrateInfos.maxAutoBitrates.audio.setValue(btr);
    };
    /**
     * Set the max buffer size for the buffer behind the current position.
     * Every buffer data before will be removed.
     * @param {Number} depthInSeconds
     */
    Player.prototype.setMaxBufferBehind = function (depthInSeconds) {
        this._priv_bufferOptions.maxBufferBehind.setValue(depthInSeconds);
    };
    /**
     * Set the max buffer size for the buffer behind the current position.
     * Every buffer data before will be removed.
     * @param {Number} depthInSeconds
     */
    Player.prototype.setMaxBufferAhead = function (depthInSeconds) {
        this._priv_bufferOptions.maxBufferAhead.setValue(depthInSeconds);
    };
    /**
     * Set the max buffer size for the buffer ahead of the current position.
     * The player will stop downloading chunks when this size is reached.
     * @param {Number} sizeInSeconds
     */
    Player.prototype.setWantedBufferAhead = function (sizeInSeconds) {
        this._priv_bufferOptions.wantedBufferAhead.setValue(sizeInSeconds);
    };
    /**
     * Set the max buffer size the buffer should take in memory
     * The player . will stop downloading chunks when this size is reached.
     * @param {Number} sizeInKBytes
     */
    Player.prototype.setMaxVideoBufferSize = function (sizeInKBytes) {
        this._priv_bufferOptions.maxVideoBufferSize.setValue(sizeInKBytes);
    };
    /**
     * Returns the max buffer size for the buffer behind the current position.
     * @returns {Number}
     */
    Player.prototype.getMaxBufferBehind = function () {
        return this._priv_bufferOptions.maxBufferBehind.getValue();
    };
    /**
     * Returns the max buffer size for the buffer behind the current position.
     * @returns {Number}
     */
    Player.prototype.getMaxBufferAhead = function () {
        return this._priv_bufferOptions.maxBufferAhead.getValue();
    };
    /**
     * Returns the max buffer size for the buffer ahead of the current position.
     * @returns {Number}
     */
    Player.prototype.getWantedBufferAhead = function () {
        return this._priv_bufferOptions.wantedBufferAhead.getValue();
    };
    /**
     * Returns the max buffer memory size for the buffer in kilobytes
     * @returns {Number}
     */
    Player.prototype.getMaxVideoBufferSize = function () {
        return this._priv_bufferOptions.maxVideoBufferSize.getValue();
    };
    /**
     * Returns type of current keysystem (e.g. playready, widevine) if the content
     * is encrypted. null otherwise.
     * @returns {string|null}
     */
    Player.prototype.getCurrentKeySystem = function () {
        if (this.videoElement === null) {
            throw new Error("Disposed player");
        }
        return getCurrentKeySystem(this.videoElement);
    };
    /**
     * Returns every available audio tracks for the current Period.
     * @returns {Array.<Object>|null}
     */
    Player.prototype.getAvailableAudioTracks = function () {
        var _a, _b;
        if (this._priv_contentInfos === null) {
            return [];
        }
        var _c = this._priv_contentInfos, currentPeriod = _c.currentPeriod, isDirectFile = _c.isDirectFile;
        if (isDirectFile) {
            return (_b = (_a = this._priv_mediaElementTrackChoiceManager) === null || _a === void 0 ? void 0 : _a.getAvailableAudioTracks()) !== null && _b !== void 0 ? _b : [];
        }
        if (this._priv_trackChoiceManager === null || currentPeriod === null) {
            return [];
        }
        return this._priv_trackChoiceManager.getAvailableAudioTracks(currentPeriod);
    };
    /**
     * Returns every available text tracks for the current Period.
     * @returns {Array.<Object>|null}
     */
    Player.prototype.getAvailableTextTracks = function () {
        var _a, _b;
        if (this._priv_contentInfos === null) {
            return [];
        }
        var _c = this._priv_contentInfos, currentPeriod = _c.currentPeriod, isDirectFile = _c.isDirectFile;
        if (isDirectFile) {
            return (_b = (_a = this._priv_mediaElementTrackChoiceManager) === null || _a === void 0 ? void 0 : _a.getAvailableTextTracks()) !== null && _b !== void 0 ? _b : [];
        }
        if (this._priv_trackChoiceManager === null || currentPeriod === null) {
            return [];
        }
        return this._priv_trackChoiceManager.getAvailableTextTracks(currentPeriod);
    };
    /**
     * Returns every available video tracks for the current Period.
     * @returns {Array.<Object>|null}
     */
    Player.prototype.getAvailableVideoTracks = function () {
        var _a, _b;
        if (this._priv_contentInfos === null) {
            return [];
        }
        var _c = this._priv_contentInfos, currentPeriod = _c.currentPeriod, isDirectFile = _c.isDirectFile;
        if (isDirectFile) {
            return (_b = (_a = this._priv_mediaElementTrackChoiceManager) === null || _a === void 0 ? void 0 : _a.getAvailableVideoTracks()) !== null && _b !== void 0 ? _b : [];
        }
        if (this._priv_trackChoiceManager === null || currentPeriod === null) {
            return [];
        }
        return this._priv_trackChoiceManager.getAvailableVideoTracks(currentPeriod);
    };
    /**
     * Returns currently chosen audio language for the current Period.
     * @returns {string}
     */
    Player.prototype.getAudioTrack = function () {
        if (this._priv_contentInfos === null) {
            return undefined;
        }
        var _a = this._priv_contentInfos, currentPeriod = _a.currentPeriod, isDirectFile = _a.isDirectFile;
        if (isDirectFile) {
            if (this._priv_mediaElementTrackChoiceManager === null) {
                return undefined;
            }
            return this._priv_mediaElementTrackChoiceManager.getChosenAudioTrack();
        }
        if (this._priv_trackChoiceManager === null || currentPeriod === null) {
            return undefined;
        }
        return this._priv_trackChoiceManager.getChosenAudioTrack(currentPeriod);
    };
    /**
     * Returns currently chosen subtitle for the current Period.
     * @returns {string}
     */
    Player.prototype.getTextTrack = function () {
        if (this._priv_contentInfos === null) {
            return undefined;
        }
        var _a = this._priv_contentInfos, currentPeriod = _a.currentPeriod, isDirectFile = _a.isDirectFile;
        if (isDirectFile) {
            if (this._priv_mediaElementTrackChoiceManager === null) {
                return undefined;
            }
            return this._priv_mediaElementTrackChoiceManager.getChosenTextTrack();
        }
        if (this._priv_trackChoiceManager === null || currentPeriod === null) {
            return undefined;
        }
        return this._priv_trackChoiceManager.getChosenTextTrack(currentPeriod);
    };
    /**
     * Returns currently chosen video track for the current Period.
     * @returns {string}
     */
    Player.prototype.getVideoTrack = function () {
        if (this._priv_contentInfos === null) {
            return undefined;
        }
        var _a = this._priv_contentInfos, currentPeriod = _a.currentPeriod, isDirectFile = _a.isDirectFile;
        if (isDirectFile) {
            if (this._priv_mediaElementTrackChoiceManager === null) {
                return undefined;
            }
            return this._priv_mediaElementTrackChoiceManager.getChosenVideoTrack();
        }
        if (this._priv_trackChoiceManager === null || currentPeriod === null) {
            return undefined;
        }
        return this._priv_trackChoiceManager.getChosenVideoTrack(currentPeriod);
    };
    /**
     * Update the audio language for the current Period.
     * @param {string} audioId
     * @throws Error - the current content has no TrackChoiceManager.
     * @throws Error - the given id is linked to no audio track.
     */
    Player.prototype.setAudioTrack = function (audioId) {
        var _a;
        if (this._priv_contentInfos === null) {
            throw new Error("No content loaded");
        }
        var _b = this._priv_contentInfos, currentPeriod = _b.currentPeriod, isDirectFile = _b.isDirectFile;
        if (isDirectFile) {
            try {
                (_a = this._priv_mediaElementTrackChoiceManager) === null || _a === void 0 ? void 0 : _a.setAudioTrackById(audioId);
                return;
            }
            catch (e) {
                throw new Error("player: unknown audio track");
            }
        }
        if (this._priv_trackChoiceManager === null || currentPeriod === null) {
            throw new Error("No compatible content launched.");
        }
        try {
            this._priv_trackChoiceManager.setAudioTrackByID(currentPeriod, audioId);
        }
        catch (e) {
            throw new Error("player: unknown audio track");
        }
    };
    /**
     * Update the text language for the current Period.
     * @param {string} sub
     * @throws Error - the current content has no TrackChoiceManager.
     * @throws Error - the given id is linked to no text track.
     */
    Player.prototype.setTextTrack = function (textId) {
        var _a;
        if (this._priv_contentInfos === null) {
            throw new Error("No content loaded");
        }
        var _b = this._priv_contentInfos, currentPeriod = _b.currentPeriod, isDirectFile = _b.isDirectFile;
        if (isDirectFile) {
            try {
                (_a = this._priv_mediaElementTrackChoiceManager) === null || _a === void 0 ? void 0 : _a.setTextTrackById(textId);
                return;
            }
            catch (e) {
                throw new Error("player: unknown text track");
            }
        }
        if (this._priv_trackChoiceManager === null || currentPeriod === null) {
            throw new Error("No compatible content launched.");
        }
        try {
            this._priv_trackChoiceManager.setTextTrackByID(currentPeriod, textId);
        }
        catch (e) {
            throw new Error("player: unknown text track");
        }
    };
    /**
     * Disable subtitles for the current content.
     */
    Player.prototype.disableTextTrack = function () {
        var _a;
        if (this._priv_contentInfos === null) {
            return;
        }
        var _b = this._priv_contentInfos, currentPeriod = _b.currentPeriod, isDirectFile = _b.isDirectFile;
        if (isDirectFile) {
            (_a = this._priv_mediaElementTrackChoiceManager) === null || _a === void 0 ? void 0 : _a.disableTextTrack();
            return;
        }
        if (this._priv_trackChoiceManager === null || currentPeriod === null) {
            return;
        }
        return this._priv_trackChoiceManager.disableTextTrack(currentPeriod);
    };
    /**
     * Update the video track for the current Period.
     * @param {string} videoId
     * @throws Error - the current content has no TrackChoiceManager.
     * @throws Error - the given id is linked to no video track.
     */
    Player.prototype.setVideoTrack = function (videoId) {
        var _a;
        if (this._priv_contentInfos === null) {
            throw new Error("No content loaded");
        }
        var _b = this._priv_contentInfos, currentPeriod = _b.currentPeriod, isDirectFile = _b.isDirectFile;
        if (isDirectFile) {
            try {
                (_a = this._priv_mediaElementTrackChoiceManager) === null || _a === void 0 ? void 0 : _a.setVideoTrackById(videoId);
                return;
            }
            catch (e) {
                throw new Error("player: unknown video track");
            }
        }
        if (this._priv_trackChoiceManager === null || currentPeriod === null) {
            throw new Error("No compatible content launched.");
        }
        try {
            this._priv_trackChoiceManager.setVideoTrackByID(currentPeriod, videoId);
        }
        catch (e) {
            throw new Error("player: unknown video track");
        }
    };
    /**
     * Disable video track for the current content.
     */
    Player.prototype.disableVideoTrack = function () {
        if (this._priv_contentInfos === null) {
            return;
        }
        var _a = this._priv_contentInfos, currentPeriod = _a.currentPeriod, isDirectFile = _a.isDirectFile;
        if (isDirectFile && this._priv_mediaElementTrackChoiceManager !== null) {
            return this._priv_mediaElementTrackChoiceManager.disableVideoTrack();
        }
        if (this._priv_trackChoiceManager === null || currentPeriod === null) {
            return;
        }
        return this._priv_trackChoiceManager.disableVideoTrack(currentPeriod);
    };
    /**
     * Returns the current list of preferred audio tracks, in preference order.
     * @returns {Array.<Object>}
     */
    Player.prototype.getPreferredAudioTracks = function () {
        return this._priv_preferredAudioTracks;
    };
    /**
     * Returns the current list of preferred text tracks, in preference order.
     * @returns {Array.<Object>}
     */
    Player.prototype.getPreferredTextTracks = function () {
        return this._priv_preferredTextTracks;
    };
    /**
     * Returns the current list of preferred text tracks, in preference order.
     * @returns {Array.<Object>}
     */
    Player.prototype.getPreferredVideoTracks = function () {
        return this._priv_preferredVideoTracks;
    };
    /**
     * Set the list of preferred audio tracks, in preference order.
     * @param {Array.<Object>} tracks
     * @param {boolean} shouldApply - `true` if those preferences should be
     * applied on the currently loaded Period. `false` if it should only
     * be applied to new content.
     */
    Player.prototype.setPreferredAudioTracks = function (tracks, shouldApply) {
        if (shouldApply === void 0) { shouldApply = false; }
        if (!Array.isArray(tracks)) {
            throw new Error("Invalid `setPreferredAudioTracks` argument. " +
                "Should have been an Array.");
        }
        this._priv_preferredAudioTracks = tracks;
        if (this._priv_trackChoiceManager !== null) {
            this._priv_trackChoiceManager.setPreferredAudioTracks(tracks, shouldApply);
        }
        else if (this._priv_mediaElementTrackChoiceManager !== null) {
            this._priv_mediaElementTrackChoiceManager.setPreferredAudioTracks(tracks, shouldApply);
        }
    };
    /**
     * Set the list of preferred text tracks, in preference order.
     * @param {Array.<Object>} tracks
     * @param {boolean} shouldApply - `true` if those preferences should be
     * applied on the currently loaded Periods. `false` if it should only
     * be applied to new content.
     */
    Player.prototype.setPreferredTextTracks = function (tracks, shouldApply) {
        if (shouldApply === void 0) { shouldApply = false; }
        if (!Array.isArray(tracks)) {
            throw new Error("Invalid `setPreferredTextTracks` argument. " +
                "Should have been an Array.");
        }
        this._priv_preferredTextTracks = tracks;
        if (this._priv_trackChoiceManager !== null) {
            this._priv_trackChoiceManager.setPreferredTextTracks(tracks, shouldApply);
        }
        else if (this._priv_mediaElementTrackChoiceManager !== null) {
            this._priv_mediaElementTrackChoiceManager.setPreferredTextTracks(tracks, shouldApply);
        }
    };
    /**
     * Set the list of preferred text tracks, in preference order.
     * @param {Array.<Object>} tracks
     * @param {boolean} shouldApply - `true` if those preferences should be
     * applied on the currently loaded Period. `false` if it should only
     * be applied to new content.
     */
    Player.prototype.setPreferredVideoTracks = function (tracks, shouldApply) {
        if (shouldApply === void 0) { shouldApply = false; }
        if (!Array.isArray(tracks)) {
            throw new Error("Invalid `setPreferredVideoTracks` argument. " +
                "Should have been an Array.");
        }
        this._priv_preferredVideoTracks = tracks;
        if (this._priv_trackChoiceManager !== null) {
            this._priv_trackChoiceManager.setPreferredVideoTracks(tracks, shouldApply);
        }
        else if (this._priv_mediaElementTrackChoiceManager !== null) {
            this._priv_mediaElementTrackChoiceManager.setPreferredVideoTracks(tracks, shouldApply);
        }
    };
    /**
     * @returns {Array.<Object>|null}
     * @deprecated
     */
    Player.prototype.getImageTrackData = function () {
        warnOnce("`getImageTrackData` is deprecated." +
            "Please use the `parseBifThumbnails` tool instead.");
        if (this._priv_contentInfos === null) {
            return null;
        }
        /* eslint-disable import/no-deprecated */
        return this._priv_contentInfos.thumbnails;
        /* eslint-enable import/no-deprecated */
    };
    /**
     * Get minimum seek-able position.
     * @returns {number}
     */
    Player.prototype.getMinimumPosition = function () {
        if (this._priv_contentInfos === null) {
            return null;
        }
        if (this._priv_contentInfos.isDirectFile) {
            return 0;
        }
        var manifest = this._priv_contentInfos.manifest;
        if (manifest !== null) {
            return manifest.getMinimumSafePosition();
        }
        return null;
    };
    /**
     * Get maximum seek-able position.
     * @returns {number}
     */
    Player.prototype.getMaximumPosition = function () {
        if (this._priv_contentInfos === null) {
            return null;
        }
        var _a = this._priv_contentInfos, isDirectFile = _a.isDirectFile, manifest = _a.manifest;
        if (isDirectFile) {
            if (this.videoElement === null) {
                throw new Error("Disposed player");
            }
            return this.videoElement.duration;
        }
        if (manifest !== null) {
            if (!manifest.isDynamic && this.videoElement !== null) {
                return this.videoElement.duration;
            }
            return manifest.getMaximumSafePosition();
        }
        return null;
    };
    /**
     * /!\ For demo use only! Do not touch!
     *
     * Returns every chunk buffered for a given buffer type.
     * Returns `null` if no SegmentBuffer was created for this type of buffer.
     * @param {string} bufferType
     * @returns {Array.<Object>|null}
     */
    Player.prototype.__priv_getSegmentBufferContent = function (bufferType) {
        if (this._priv_contentInfos === null ||
            this._priv_contentInfos.segmentBuffersStore === null) {
            return null;
        }
        var segmentBufferStatus = this._priv_contentInfos
            .segmentBuffersStore.getStatus(bufferType);
        return segmentBufferStatus.type === "initialized" ?
            segmentBufferStatus.value.getInventory() :
            null;
    };
    /**
     * Reset all state properties relative to a playing content.
     */
    Player.prototype._priv_cleanUpCurrentContentState = function () {
        var _this = this;
        var _a;
        log.debug("Locking `contentLock` to clean-up the current content.");
        // lock playback of new contents while cleaning up is pending
        this._priv_contentLock.setValue(true);
        this._priv_contentInfos = null;
        this._priv_trackChoiceManager = null;
        (_a = this._priv_mediaElementTrackChoiceManager) === null || _a === void 0 ? void 0 : _a.dispose();
        this._priv_mediaElementTrackChoiceManager = null;
        this._priv_contentEventsMemory = {};
        // DRM-related clean-up
        var freeUpContentLock = function () {
            if (_this.videoElement !== null) { // If not disposed
                log.debug("Unlocking `contentLock`. Next content can begin.");
                _this._priv_contentLock.setValue(false);
            }
        };
        if (!isNullOrUndefined(this.videoElement)) {
            clearOnStop(this.videoElement).then(function () {
                log.debug("API: DRM session cleaned-up with success!");
                freeUpContentLock();
            }, function (err) {
                log.error("API: An error arised when trying to clean-up the DRM session:" +
                    (err instanceof Error ? err.toString() :
                        "Unknown Error"));
                freeUpContentLock();
            });
        }
        else {
            freeUpContentLock();
        }
    };
    /**
     * Triggered each time the playback Observable emits.
     *
     * React to various events.
     *
     * @param {Object} event - payload emitted
     */
    Player.prototype._priv_onPlaybackEvent = function (event) {
        switch (event.type) {
            case "inband-events":
                var inbandEvents = event.value;
                this.trigger("inbandEvents", inbandEvents);
                return;
            case "stream-event":
                this.trigger("streamEvent", event.value);
                break;
            case "stream-event-skip":
                this.trigger("streamEventSkip", event.value);
                break;
            case "activePeriodChanged":
                this._priv_onActivePeriodChanged(event.value);
                break;
            case "periodStreamReady":
                this._priv_onPeriodStreamReady(event.value);
                break;
            case "periodStreamCleared":
                this._priv_onPeriodStreamCleared(event.value);
                break;
            case "reloading-media-source":
                this._priv_onReloadingMediaSource();
                break;
            case "representationChange":
                this._priv_onRepresentationChange(event.value);
                break;
            case "adaptationChange":
                this._priv_onAdaptationChange(event.value);
                break;
            case "bitrateEstimationChange":
                this._priv_onBitrateEstimationChange(event.value);
                break;
            case "manifestReady":
                this._priv_onManifestReady(event.value);
                break;
            case "warning":
                this._priv_onPlaybackWarning(event.value);
                break;
            case "loaded":
                if (this._priv_contentInfos === null) {
                    log.error("API: Loaded event while no content is loaded");
                    return;
                }
                this._priv_contentInfos.segmentBuffersStore = event.value.segmentBuffersStore;
                break;
            case "decipherabilityUpdate":
                this.trigger("decipherabilityUpdate", event.value);
                break;
            case "added-segment":
                if (this._priv_contentInfos === null) {
                    log.error("API: Added segment while no content is loaded");
                    return;
                }
                // Manage image tracks
                // @deprecated
                var _a = event.value, content = _a.content, segmentData = _a.segmentData;
                if (content.adaptation.type === "image") {
                    if (!isNullOrUndefined(segmentData) &&
                        segmentData.type === "bif") {
                        var imageData = segmentData.data;
                        /* eslint-disable import/no-deprecated */
                        this._priv_contentInfos.thumbnails = imageData;
                        this.trigger("imageTrackUpdate", { data: this._priv_contentInfos.thumbnails });
                        /* eslint-enable import/no-deprecated */
                    }
                }
        }
    };
    /**
     * Triggered when we received a fatal error.
     * Clean-up ressources and signal that the content has stopped on error.
     * @param {Error} error
     */
    Player.prototype._priv_onPlaybackError = function (error) {
        var formattedError = formatError(error, {
            defaultCode: "NONE",
            defaultReason: "An unknown error stopped content playback.",
        });
        formattedError.fatal = true;
        if (this._priv_contentInfos !== null) {
            this._priv_contentInfos.currentContentCanceller.cancel();
        }
        this._priv_cleanUpCurrentContentState();
        this._priv_currentError = formattedError;
        log.error("API: The player stopped because of an error", error instanceof Error ? error : "");
        this._priv_setPlayerState(PLAYER_STATES.STOPPED);
        // TODO This condition is here because the eventual callback called when the
        // player state is updated can launch a new content, thus the error will not
        // be here anymore, in which case triggering the "error" event is unwanted.
        // This is very ugly though, and we should probable have a better solution
        if (this._priv_currentError === formattedError) {
            this.trigger("error", formattedError);
        }
    };
    /**
     * Triggered when we received a warning event during playback.
     * Trigger the right API event.
     * @param {Error} error
     */
    Player.prototype._priv_onPlaybackWarning = function (error) {
        var formattedError = formatError(error, {
            defaultCode: "NONE",
            defaultReason: "An unknown error happened.",
        });
        log.warn("API: Sending warning:", formattedError);
        this.trigger("warning", formattedError);
    };
    /**
     * Triggered when the Manifest has been loaded for the current content.
     * Initialize various private properties and emit initial event.
     * @param {Object} value
     */
    Player.prototype._priv_onManifestReady = function (_a) {
        var _this = this;
        var manifest = _a.manifest;
        var contentInfos = this._priv_contentInfos;
        if (contentInfos === null) {
            log.error("API: The manifest is loaded but no content is.");
            return;
        }
        contentInfos.manifest = manifest;
        this._priv_lastContentPlaybackInfos.manifest = manifest;
        var initialAudioTrack = contentInfos.initialAudioTrack, initialTextTrack = contentInfos.initialTextTrack;
        this._priv_trackChoiceManager = new TrackChoiceManager({
            preferTrickModeTracks: this._priv_preferTrickModeTracks,
        });
        var preferredAudioTracks = initialAudioTrack === undefined ?
            this._priv_preferredAudioTracks :
            [initialAudioTrack];
        this._priv_trackChoiceManager.setPreferredAudioTracks(preferredAudioTracks, true);
        var preferredTextTracks = initialTextTrack === undefined ?
            this._priv_preferredTextTracks :
            [initialTextTrack];
        this._priv_trackChoiceManager.setPreferredTextTracks(preferredTextTracks, true);
        this._priv_trackChoiceManager.setPreferredVideoTracks(this._priv_preferredVideoTracks, true);
        manifest.addEventListener("manifestUpdate", function () {
            // Update the tracks chosen if it changed
            if (_this._priv_trackChoiceManager !== null) {
                _this._priv_trackChoiceManager.update();
            }
        }, contentInfos.currentContentCanceller.signal);
    };
    /**
     * Triggered each times the current Period Changed.
     * Store and emit initial state for the Period.
     *
     * @param {Object} value
     */
    Player.prototype._priv_onActivePeriodChanged = function (_a) {
        var _b, _c, _d, _e, _f, _g;
        var period = _a.period;
        if (this._priv_contentInfos === null) {
            log.error("API: The active period changed but no content is loaded");
            return;
        }
        this._priv_contentInfos.currentPeriod = period;
        if (this._priv_contentEventsMemory.periodChange !== period) {
            this._priv_contentEventsMemory.periodChange = period;
            this.trigger("periodChange", period);
        }
        this.trigger("availableAudioTracksChange", this.getAvailableAudioTracks());
        this.trigger("availableTextTracksChange", this.getAvailableTextTracks());
        this.trigger("availableVideoTracksChange", this.getAvailableVideoTracks());
        // Emit intial events for the Period
        if (this._priv_trackChoiceManager !== null) {
            var audioTrack = this._priv_trackChoiceManager.getChosenAudioTrack(period);
            var textTrack = this._priv_trackChoiceManager.getChosenTextTrack(period);
            var videoTrack = this._priv_trackChoiceManager.getChosenVideoTrack(period);
            this.trigger("audioTrackChange", audioTrack);
            this.trigger("textTrackChange", textTrack);
            this.trigger("videoTrackChange", videoTrack);
        }
        else {
            this.trigger("audioTrackChange", null);
            this.trigger("textTrackChange", null);
            this.trigger("videoTrackChange", null);
        }
        this._priv_triggerAvailableBitratesChangeEvent("availableAudioBitratesChange", this.getAvailableAudioBitrates());
        this._priv_triggerAvailableBitratesChangeEvent("availableVideoBitratesChange", this.getAvailableVideoBitrates());
        var audioBitrate = (_d = (_c = (_b = this._priv_getCurrentRepresentations()) === null || _b === void 0 ? void 0 : _b.audio) === null || _c === void 0 ? void 0 : _c.bitrate) !== null && _d !== void 0 ? _d : -1;
        this._priv_triggerCurrentBitrateChangeEvent("audioBitrateChange", audioBitrate);
        var videoBitrate = (_g = (_f = (_e = this._priv_getCurrentRepresentations()) === null || _e === void 0 ? void 0 : _e.video) === null || _f === void 0 ? void 0 : _f.bitrate) !== null && _g !== void 0 ? _g : -1;
        this._priv_triggerCurrentBitrateChangeEvent("videoBitrateChange", videoBitrate);
    };
    /**
     * Triggered each times a new "PeriodStream" is ready.
     * Choose the right Adaptation for the Period and emit it.
     * @param {Object} value
     */
    Player.prototype._priv_onPeriodStreamReady = function (value) {
        var type = value.type, period = value.period, adaptation$ = value.adaptation$;
        switch (type) {
            case "video":
                if (this._priv_trackChoiceManager === null) {
                    log.error("API: TrackChoiceManager not instanciated for a new video period");
                    adaptation$.next(null);
                }
                else {
                    this._priv_trackChoiceManager.addPeriod(type, period, adaptation$);
                    this._priv_trackChoiceManager.setInitialVideoTrack(period);
                }
                break;
            case "audio":
                if (this._priv_trackChoiceManager === null) {
                    log.error("API: TrackChoiceManager not instanciated for a new ".concat(type, " period"));
                    adaptation$.next(null);
                }
                else {
                    this._priv_trackChoiceManager.addPeriod(type, period, adaptation$);
                    this._priv_trackChoiceManager.setInitialAudioTrack(period);
                }
                break;
            case "text":
                if (this._priv_trackChoiceManager === null) {
                    log.error("API: TrackChoiceManager not instanciated for a new ".concat(type, " period"));
                    adaptation$.next(null);
                }
                else {
                    this._priv_trackChoiceManager.addPeriod(type, period, adaptation$);
                    this._priv_trackChoiceManager.setInitialTextTrack(period);
                }
                break;
            default:
                var adaptations = period.adaptations[type];
                if (!isNullOrUndefined(adaptations) && adaptations.length > 0) {
                    adaptation$.next(adaptations[0]);
                }
                else {
                    adaptation$.next(null);
                }
                break;
        }
    };
    /**
     * Triggered each times we "remove" a PeriodStream.
     * @param {Object} value
     */
    Player.prototype._priv_onPeriodStreamCleared = function (value) {
        var type = value.type, period = value.period;
        // Clean-up track choice from TrackChoiceManager
        switch (type) {
            case "audio":
            case "text":
            case "video":
                if (this._priv_trackChoiceManager !== null) {
                    this._priv_trackChoiceManager.removePeriod(type, period);
                }
                break;
        }
        // Clean-up stored Representation and Adaptation information
        if (this._priv_contentInfos === null) {
            return;
        }
        var _a = this._priv_contentInfos, activeAdaptations = _a.activeAdaptations, activeRepresentations = _a.activeRepresentations;
        if (!isNullOrUndefined(activeAdaptations) &&
            !isNullOrUndefined(activeAdaptations[period.id])) {
            var activePeriodAdaptations = activeAdaptations[period.id];
            delete activePeriodAdaptations[type];
            if (Object.keys(activePeriodAdaptations).length === 0) {
                delete activeAdaptations[period.id];
            }
        }
        if (!isNullOrUndefined(activeRepresentations) &&
            !isNullOrUndefined(activeRepresentations[period.id])) {
            var activePeriodRepresentations = activeRepresentations[period.id];
            delete activePeriodRepresentations[type];
            if (Object.keys(activePeriodRepresentations).length === 0) {
                delete activeRepresentations[period.id];
            }
        }
    };
    /**
     * Triggered each time the content is re-loaded on the MediaSource.
     */
    Player.prototype._priv_onReloadingMediaSource = function () {
        if (this._priv_contentInfos !== null) {
            this._priv_contentInfos.segmentBuffersStore = null;
        }
        if (this._priv_trackChoiceManager !== null) {
            this._priv_trackChoiceManager.resetPeriods();
        }
    };
    /**
     * Triggered each times a new Adaptation is considered for the current
     * content.
     * Store given Adaptation and emit it if from the current Period.
     * @param {Object} value
     */
    Player.prototype._priv_onAdaptationChange = function (_a) {
        var _b;
        var type = _a.type, adaptation = _a.adaptation, period = _a.period;
        if (this._priv_contentInfos === null) {
            log.error("API: The adaptations changed but no content is loaded");
            return;
        }
        // lazily create this._priv_contentInfos.activeAdaptations
        if (this._priv_contentInfos.activeAdaptations === null) {
            this._priv_contentInfos.activeAdaptations = {};
        }
        var _c = this._priv_contentInfos, activeAdaptations = _c.activeAdaptations, currentPeriod = _c.currentPeriod;
        var activePeriodAdaptations = activeAdaptations[period.id];
        if (isNullOrUndefined(activePeriodAdaptations)) {
            activeAdaptations[period.id] = (_b = {}, _b[type] = adaptation, _b);
        }
        else {
            activePeriodAdaptations[type] = adaptation;
        }
        if (this._priv_trackChoiceManager !== null &&
            currentPeriod !== null && !isNullOrUndefined(period) &&
            period.id === currentPeriod.id) {
            switch (type) {
                case "audio":
                    var audioTrack = this._priv_trackChoiceManager
                        .getChosenAudioTrack(currentPeriod);
                    this.trigger("audioTrackChange", audioTrack);
                    var availableAudioBitrates = this.getAvailableAudioBitrates();
                    this._priv_triggerAvailableBitratesChangeEvent("availableAudioBitratesChange", availableAudioBitrates);
                    break;
                case "text":
                    var textTrack = this._priv_trackChoiceManager
                        .getChosenTextTrack(currentPeriod);
                    this.trigger("textTrackChange", textTrack);
                    break;
                case "video":
                    var videoTrack = this._priv_trackChoiceManager
                        .getChosenVideoTrack(currentPeriod);
                    this.trigger("videoTrackChange", videoTrack);
                    var availableVideoBitrates = this.getAvailableVideoBitrates();
                    this._priv_triggerAvailableBitratesChangeEvent("availableVideoBitratesChange", availableVideoBitrates);
                    break;
            }
        }
    };
    /**
     * Triggered each times a new Representation is considered during playback.
     *
     * Store given Representation and emit it if from the current Period.
     *
     * @param {Object} obj
     */
    Player.prototype._priv_onRepresentationChange = function (_a) {
        var _b;
        var _c;
        var type = _a.type, period = _a.period, representation = _a.representation;
        if (this._priv_contentInfos === null) {
            log.error("API: The representations changed but no content is loaded");
            return;
        }
        // lazily create this._priv_contentInfos.activeRepresentations
        if (this._priv_contentInfos.activeRepresentations === null) {
            this._priv_contentInfos.activeRepresentations = {};
        }
        var _d = this._priv_contentInfos, activeRepresentations = _d.activeRepresentations, currentPeriod = _d.currentPeriod;
        var activePeriodRepresentations = activeRepresentations[period.id];
        if (isNullOrUndefined(activePeriodRepresentations)) {
            activeRepresentations[period.id] = (_b = {}, _b[type] = representation, _b);
        }
        else {
            activePeriodRepresentations[type] = representation;
        }
        var bitrate = (_c = representation === null || representation === void 0 ? void 0 : representation.bitrate) !== null && _c !== void 0 ? _c : -1;
        if (!isNullOrUndefined(period) &&
            currentPeriod !== null &&
            currentPeriod.id === period.id) {
            if (type === "video") {
                this._priv_triggerCurrentBitrateChangeEvent("videoBitrateChange", bitrate);
            }
            else if (type === "audio") {
                this._priv_triggerCurrentBitrateChangeEvent("audioBitrateChange", bitrate);
            }
        }
    };
    /**
     * Triggered each time a bitrate estimate is calculated.
     *
     * Emit it.
     *
     * @param {Object} value
     */
    Player.prototype._priv_onBitrateEstimationChange = function (_a) {
        var type = _a.type, bitrate = _a.bitrate;
        if (bitrate !== undefined) {
            this._priv_bitrateInfos.lastBitrates[type] = bitrate;
        }
        this.trigger("bitrateEstimationChange", { type: type, bitrate: bitrate });
    };
    /**
     * Triggered each time a textTrack is added to the video DOM Element.
     *
     * Trigger the right Player Event.
     *
     * @param {Array.<TextTrackElement>} tracks
     */
    Player.prototype._priv_onNativeTextTracksNext = function (tracks) {
        this.trigger("nativeTextTracksChange", tracks);
    };
    /**
     * Triggered each time the player state updates.
     *
     * Trigger the right Player Event.
     *
     * @param {string} newState
     */
    Player.prototype._priv_setPlayerState = function (newState) {
        if (this.state !== newState) {
            this.state = newState;
            log.info("API: playerStateChange event", newState);
            this.trigger("playerStateChange", newState);
        }
    };
    /**
     * Triggered each time a playback observation.
     *
     * Trigger the right Player Event
     *
     * @param {Object} observation
     */
    Player.prototype._priv_triggerPositionUpdate = function (observation) {
        var _a;
        if (this._priv_contentInfos === null) {
            log.warn("API: Cannot perform time update: no content loaded.");
            return;
        }
        if (this.state === PLAYER_STATES.RELOADING) {
            return;
        }
        var _b = this._priv_contentInfos, isDirectFile = _b.isDirectFile, manifest = _b.manifest;
        if ((!isDirectFile && manifest === null) || isNullOrUndefined(observation)) {
            return;
        }
        this._priv_lastContentPlaybackInfos.lastPlaybackPosition = observation.position;
        var maximumPosition = manifest !== null ? manifest.getMaximumSafePosition() :
            undefined;
        var positionData = {
            position: observation.position,
            duration: observation.duration,
            playbackRate: observation.playbackRate,
            maximumBufferTime: maximumPosition,
            // TODO fix higher up?
            bufferGap: isFinite(observation.bufferGap) ? observation.bufferGap :
                0,
        };
        if (manifest !== null &&
            manifest.isLive &&
            observation.position > 0) {
            var ast = (_a = manifest.availabilityStartTime) !== null && _a !== void 0 ? _a : 0;
            positionData.wallClockTime = observation.position + ast;
            var livePosition = manifest.getLivePosition();
            if (livePosition !== undefined) {
                positionData.liveGap = livePosition - observation.position;
            }
        }
        else if (isDirectFile && this.videoElement !== null) {
            var startDate = getStartDate(this.videoElement);
            if (startDate !== undefined) {
                positionData.wallClockTime = startDate + observation.position;
            }
        }
        this.trigger("positionUpdate", positionData);
    };
    /**
     * Trigger one of the "availableBitratesChange" event only if it changed from
     * the previously stored value.
     * @param {string} event
     * @param {Array.<number>} newVal
     */
    Player.prototype._priv_triggerAvailableBitratesChangeEvent = function (event, newVal) {
        var prevVal = this._priv_contentEventsMemory[event];
        if (prevVal === undefined || !areArraysOfNumbersEqual(newVal, prevVal)) {
            this._priv_contentEventsMemory[event] = newVal;
            this.trigger(event, newVal);
        }
    };
    /**
     * Trigger one of the "bitrateChange" event only if it changed from the
     * previously stored value.
     * @param {string} event
     * @param {number} newVal
     */
    Player.prototype._priv_triggerCurrentBitrateChangeEvent = function (event, newVal) {
        if (newVal !== this._priv_contentEventsMemory[event]) {
            this._priv_contentEventsMemory[event] = newVal;
            this.trigger(event, newVal);
        }
    };
    Player.prototype._priv_getCurrentRepresentations = function () {
        if (this._priv_contentInfos === null) {
            return null;
        }
        var _a = this._priv_contentInfos, currentPeriod = _a.currentPeriod, activeRepresentations = _a.activeRepresentations;
        if (currentPeriod === null ||
            activeRepresentations === null ||
            isNullOrUndefined(activeRepresentations[currentPeriod.id])) {
            return null;
        }
        return activeRepresentations[currentPeriod.id];
    };
    return Player;
}(EventEmitter));
Player.version = /* PLAYER_VERSION */ "3.28.0-samsung.seek3";
export default Player;
