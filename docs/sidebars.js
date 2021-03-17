module.exports = {
  api: [
    {
      type: "doc",
      id: "api/player_options",
    },
    {
      type: "doc",
      id: "api/player_states",
    },
    {
      type: "doc",
      id: "api/player_events",
    },
    {
      type: "doc",
      id: "api/player_errors_warning",
    },
    {
      type: "category",
      label: "Basic methods",
      items: [
        "api/basicMethods/loadVideo-api",
        "api/basicMethods/reload-api",
        "api/basicMethods/getPlayerState-api",
        "api/basicMethods/addEventListener-api",
        "api/basicMethods/removeEventListener-api",
        "api/basicMethods/play-api",
        "api/basicMethods/pause-api",
        "api/basicMethods/stop-api",
        "api/basicMethods/getPosition-api",
        "api/basicMethods/getWallClockTime-api",
        "api/basicMethods/seekTo-api",
        "api/basicMethods/getMinimumPosition-api",
        "api/basicMethods/getMaximumPosition-api",
        "api/basicMethods/getVideoDuration-api",
        "api/basicMethods/getError-api",
        "api/basicMethods/getVideoElement-api",
        "api/basicMethods/dispose-api",
      ],
    },
    {
      type: "category",
      label: "Speed Control",
      items: [
        "api/speedControl/setPlaybackRate-api",
        "api/speedControl/getPlaybackRate-api",
      ],
    },
    {
      type: "category",
      label: "Volume Control",
      items: [
        "api/volumeControl/setVolume-api",
        "api/volumeControl/getVolume-api",
        "api/volumeControl/mute-api",
        "api/volumeControl/unMute-api",
        "api/volumeControl/isMute-api",
      ],
    },
    {
      type: "category",
      label: "Track Selection",
      items: [
        "api/trackSelection/getAudioTrack-api",
        "api/trackSelection/getTextTrack-api",
        "api/trackSelection/getVideoTrack-api",
        "api/trackSelection/getAvailableAudioTracks-api",
        "api/trackSelection/getAvailableTextTracks-api",
        "api/trackSelection/getAvailableVideoTracks-api",
        "api/trackSelection/setAudioTrack-api",
        "api/trackSelection/setTextTrack-api",
        "api/trackSelection/disableTextTrack-api",
        "api/trackSelection/setVideoTrack-api",
        "api/trackSelection/disableVideoTrack-api",
        "api/trackSelection/setPreferredAudioTracks-api",
        "api/trackSelection/getPreferredAudioTracks-api",
        "api/trackSelection/setPreferredTextTracks-api",
        "api/trackSelection/getPreferredTextTracks-api",
        "api/trackSelection/setPreferredVideoTracks-api",
        "api/trackSelection/getPreferredVideoTracks-api",
      ],
    },
    {
      type: "category",
      label: "Bitrate selection",
      items: [
        "api/bitrateSelection/getAvailableVideoBitrates-api",
        "api/bitrateSelection/getAvailableAudioBitrates-api",
        "api/bitrateSelection/getVideoBitrate-api",
        "api/bitrateSelection/getAudioBitrate-api",
        "api/bitrateSelection/setMinVideoBitrate-api",
        "api/bitrateSelection/setMinAudioBitrate-api",
        "api/bitrateSelection/getMinVideoBitrate-api",
        "api/bitrateSelection/getMinAudioBitrate-api",
        "api/bitrateSelection/setMaxVideoBitrate-api",
        "api/bitrateSelection/setMaxAudioBitrate-api",
        "api/bitrateSelection/getMaxVideoBitrate-api",
        "api/bitrateSelection/getMaxAudioBitrate-api",
        "api/bitrateSelection/setVideoBitrate-api",
        "api/bitrateSelection/setAudioBitrate-api",
        "api/bitrateSelection/getManualVideoBitrate-api",
        "api/bitrateSelection/getManualAudioBitrate-api",
      ],
    },
    {
      type: "category",
      label: "Buffer control",
      items: [
        "api/bufferControl/setWantedBufferAhead-api",
        "api/bufferControl/getWantedBufferAhead-api",
        "api/bufferControl/setMaxBufferBehind-api",
        "api/bufferControl/getMaxBufferBehind-api",
        "api/bufferControl/setMaxBufferAhead-api",
        "api/bufferControl/getMaxBufferAhead-api",
      ],
    },
    {
      type: "category",
      label: "Buffer information",
      items: [
        "api/bufferInformation/getVideoLoadedTime-api",
        "api/bufferInformation/getVideoPlayedTime-api",
        "api/bufferInformation/getVideoBufferGap-api",
      ],
    },
    {
      type: "category",
      label: "Content information",
      items: [
        "api/contentInformation/isLive-api",
        "api/contentInformation/getUrl-api",
        "api/contentInformation/getCurrentKeySystem-api",
      ],
    },
    {
      type: "category",
      label: "Deprecated",
      items: [
        "api/deprecated/getManifest-api",
        "api/deprecated/getCurrentAdaptations-api",
        "api/deprecated/getCurrentRepresentations-api",
        "api/deprecated/getImageTrackData-api",
        "api/deprecated/setFullscreen-api",
        "api/deprecated/exitFullscreen-api",
        "api/deprecated/isFullscreen-api",
        "api/deprecated/getNativeTextTrack-api",
      ],
    },
    {
      type: "category",
      label: "Static properties",
      items: [
        "api/staticProperties/version-api",
        "api/staticProperties/ErrorTypes-api",
        "api/staticProperties/ErrorCodes-api",
        "api/staticProperties/LogLevel-api",
      ],
    },
    {
      type: "category",
      label: "Exported Tools",
      items: [
        "api/tools/StringUtils-tool",
        "api/tools/TextTrackRenderer-tool",
        "api/tools/MediaCapabilitiesProber-tool",
        "api/tools/parseBifThumbnails-tool",
        "api/tools/createMetaplaylist-tool",
      ],
    },
    {
      type: "doc",
      id: "api/images",
    },
    {
      type: "doc",
      id: "api/plugins",
    },
  ],
  getstarted: [
    {
      type: "category",
      label: "Tutorials",
      items: [
        "tutorials/quickStart-tutorials",
        "tutorials/drm-tutorials",
        "tutorials/selectingTrack-tutorials",
        "tutorials/listenSteamEvents-tutorials",
      ],
    },
    {
      type: "category",
      label: "Additional ressources",
      items: [
        "additional_ressources/manifest",
        "additional_ressources/text_tracks",
        "additional_ressources/dash_rxplayer_adaptation_difference",
        "additional_ressources/metaplaylist",
        "additional_ressources/local_contents",
        "additional_ressources/local_manifest_v0.1",
        "additional_ressources/minimal_player",
        "additional_ressources/low_latency",
        "additional_ressources/initial_position",
        "additional_ressources/presentation_time_offset",
        "additional_ressources/deprecated_api",
      ],
    },
    {
      type: "doc",
      id: "glossary",
    },
  ],
};
