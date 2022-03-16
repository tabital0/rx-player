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
import log from "../../log";
import objectValues from "../../utils/object_values";
/**
 * Store information about pending requests, like information about:
 *   - for which segments they are
 *   - how the request's progress goes
 * @class PendingRequestsStore
 */
var PendingRequestsStore = /** @class */ (function () {
    function PendingRequestsStore() {
        this._currentRequests = {};
    }
    /**
     * Add information about a new pending request.
     * @param {string} id
     * @param {Object} payload
     */
    PendingRequestsStore.prototype.add = function (payload) {
        var id = payload.id, time = payload.time, duration = payload.duration, requestTimestamp = payload.requestTimestamp;
        this._currentRequests[id] = { time: time,
            duration: duration,
            requestTimestamp: requestTimestamp,
            progress: [] };
    };
    /**
     * Notify of the progress of a currently pending request.
     * @param {Object} progress
     */
    PendingRequestsStore.prototype.addProgress = function (progress) {
        var request = this._currentRequests[progress.id];
        if (request == null) {
            if (false) {
                throw new Error("ABR: progress for a request not added");
            }
            log.warn("ABR: progress for a request not added");
            return;
        }
        request.progress.push(progress);
    };
    /**
     * Remove a request previously set as pending.
     * @param {string} id
     */
    PendingRequestsStore.prototype.remove = function (id) {
        if (this._currentRequests[id] == null) {
            if (false) {
                throw new Error("ABR: can't remove unknown request");
            }
            log.warn("ABR: can't remove unknown request");
        }
        delete this._currentRequests[id];
    };
    /**
     * Returns information about all pending requests, in segment's chronological
     * order.
     * @returns {Array.<Object>}
     */
    PendingRequestsStore.prototype.getRequests = function () {
        return objectValues(this._currentRequests)
            .filter(function (x) { return x != null; })
            .sort(function (reqA, reqB) { return reqA.time - reqB.time; });
    };
    return PendingRequestsStore;
}());
export default PendingRequestsStore;
