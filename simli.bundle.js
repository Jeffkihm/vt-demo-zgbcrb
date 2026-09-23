/* simli-client 3.0.2, CJS->browser bundle (case-fix Client->client). livekit-client from window.LivekitClient */
(function(){var M={},C={};
function norm(from,req){if(req==="livekit-client")return "livekit-client";var base=from.split("/").slice(0,-1);req.split("/").forEach(function(p){if(p===".")return;if(p==="..")base.pop();else base.push(p);});var n=base.join("/");if(!/\.js$/.test(n))n+=".js";if(n==="Client.js")n="client.js";return n;}
function R(from){return function(req){var k=norm(from,req);if(k==="livekit-client")return window.LivekitClient;if(C[k])return C[k].exports;if(!M[k])throw new Error("simli bundle: missing "+k);var m={exports:{}};C[k]=m;M[k](m,m.exports,R(k));return m.exports;};}
M["index.js"]=function(module,exports,require){
"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.LogLevel = exports.generateIceServers = exports.generateSimliSessionToken = exports.SimliClient = void 0;
var Client_1 = require("./Client");
Object.defineProperty(exports, "SimliClient", { enumerable: true, get: function () { return Client_1.SimliClient; } });
Object.defineProperty(exports, "generateSimliSessionToken", { enumerable: true, get: function () { return Client_1.generateSimliSessionToken; } });
Object.defineProperty(exports, "generateIceServers", { enumerable: true, get: function () { return Client_1.generateIceServers; } });
Object.defineProperty(exports, "LogLevel", { enumerable: true, get: function () { return Client_1.LogLevel; } });

};
M["client.js"]=function(module,exports,require){
"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.LogLevel = exports.Logger = exports.SimliClient = void 0;
exports.generateSimliSessionToken = generateSimliSessionToken;
exports.generateIceServers = generateIceServers;
const LivekitTransport_1 = require("./Transports/LivekitTransport");
const P2PTransport_1 = require("./Transports/P2PTransport");
const Logger_1 = require("./Logger");
Object.defineProperty(exports, "Logger", { enumerable: true, get: function () { return Logger_1.Logger; } });
Object.defineProperty(exports, "LogLevel", { enumerable: true, get: function () { return Logger_1.LogLevel; } });
const AudioProcessor = (buffer) => {
    if (buffer <= 0) {
        throw "Invalid Buffer Size, Can't be negative";
    }
    if (Math.floor(buffer) - buffer != 0) {
        throw "Invalid Buffer Size, Can't be a float";
    }
    return `
        class AudioProcessor extends AudioWorkletProcessor {
          constructor() {
            super();
            this.buffer = new Int16Array(${buffer});
            this.bufferIndex = 0;
        }

          process(inputs, outputs, parameters) {
            const input = inputs[0];
            const inputChannel = input[0];
            if (inputChannel) {
              for (let i = 0; i < inputChannel.length; i++) {
                this.buffer[this.bufferIndex] = Math.max(-32768, Math.min(32767, Math.round(inputChannel[i] * 32767)));
                this.bufferIndex++;

                if (this.bufferIndex === this.buffer.length){
                  this.port.postMessage({type: 'audioData', data: this.buffer.slice(0, this.bufferIndex)});
                  this.bufferIndex = 0;
                }
              }
            }
            return true;
          }
        }

        registerProcessor('audio-processor', AudioProcessor);
      `;
};
async function generateSimliSessionToken(request, SimliURL = "https://api.simli.ai") {
    const url = `${SimliURL}/compose/token`;
    const response = await fetch(url, {
        method: "POST",
        body: JSON.stringify(request.config),
        headers: {
            "Content-Type": "application/json",
            "x-simli-api-key": request.apiKey
        },
    });
    if (!response.ok) {
        const errorText = await response.text();
        throw errorText;
    }
    const resJSON = await response.json();
    return resJSON;
}
async function generateIceServers(apiKey, SimliURL = "https://api.simli.ai") {
    try {
        const url = `${SimliURL}/compose/ice`;
        const response = await fetch(url, {
            headers: {
                "Content-Type": "application/json",
                "x-simli-api-key": apiKey,
            },
            method: "GET",
        });
        if (!response.ok) {
            throw new Error(`SIMLI: HTTP error! status: ${response.status}`);
        }
        const iceServers = await response.json();
        if (!iceServers || iceServers.length === 0) {
            throw new Error("SIMLI: No ICE servers returned");
        }
        return iceServers;
    }
    catch (error) {
        return [{ urls: ["stun:stun.l.google.com:19302"] }];
    }
}
class SimliClient {
    session_token;
    transport = "livekit";
    signaling = "websockets";
    videoElement;
    audioElement;
    audioBufferSize = 3000;
    connection;
    connectionTimeout;
    connectionResolve;
    connectionReject;
    connectionPromise;
    sourceNode = null;
    audioWorklet = null;
    MAX_RETRY_ATTEMPTS = 10;
    RETRY_DELAY = 2000;
    CONNECTION_TIMEOUT_MS = 15000;
    retryAttempt = 0;
    SimliWSURL = "wss://api.simli.ai";
    audioContext = new (window.AudioContext ||
        window.webkitAudioContext)({
        sampleRate: 16000,
    });
    logger;
    iceServers;
    persistent_events;
    failReason = null;
    shouldStop = false;
    // Type-safe event methods
    on(event, callback) {
        if (!this.persistent_events.has(event)) {
            this.persistent_events.set(event, new Set());
        }
        this.persistent_events.get(event)?.add(callback);
        this.logger.debug("Registered Callback for Event: " + event);
        this.connection.on(event, callback);
    }
    off(event, callback) {
        if (!this.persistent_events.has(event)) {
            throw "Event Not Regsitered";
        }
        this.persistent_events.get(event)?.delete(callback);
        this.connection.off(event, callback);
    }
    constructor(session_token, videoElement, audioElement, iceServers, logLevel = Logger_1.LogLevel.DEBUG, transport_mode = "p2p", signaling = "websockets", SimliWSURL = "wss://api.simli.ai", audioBufferSize = 3000) {
        if (audioBufferSize <= 0) {
            throw "Invalid Buffer Size, Can't be negative";
        }
        if (Math.floor(audioBufferSize) - audioBufferSize != 0) {
            throw "Invalid Buffer Size, Can't be a float";
        }
        if (!(SimliWSURL.startsWith("ws://") || SimliWSURL.startsWith("wss://")) || SimliWSURL.endsWith("/")) {
            throw "Invalid Simli WS URL";
        }
        this.audioBufferSize = audioBufferSize;
        this.session_token = session_token;
        this.transport = transport_mode;
        this.signaling = signaling;
        this.SimliWSURL = SimliWSURL;
        this.videoElement = videoElement;
        this.audioElement = audioElement;
        this.iceServers = iceServers;
        this.logger = new Logger_1.Logger(logLevel);
        let resolveFn;
        let rejectFn;
        this.connectionPromise = new Promise((resolve, reject) => {
            resolveFn = resolve;
            rejectFn = reject;
        });
        this.connectionResolve = resolveFn;
        this.connectionReject = rejectFn;
        this.persistent_events = new Map();
        this.connectionTimeout = setTimeout(() => this.connectionReject("CONNECTION TIMED OUT"), this.CONNECTION_TIMEOUT_MS);
        switch (this.transport) {
            case "livekit":
                this.connection = new LivekitTransport_1.LivekitTransport(this.SimliWSURL, this.session_token, videoElement, audioElement, this.logger, this.connectionReject);
                break;
            case "p2p":
                if (!iceServers || iceServers.length == 0) {
                    throw "Ice Servers Required for P2P Mode";
                }
                this.connection = new P2PTransport_1.P2PTransport(this.SimliWSURL, this.session_token, true, iceServers, videoElement, audioElement, this.logger, this.connectionReject);
                break;
            default:
                throw new Error("Not Implemented Yet");
        }
        this.connection.on("start", () => {
            this.connectionResolve();
            clearTimeout(this.connectionTimeout);
        });
        this.connection.on("unknown", (message) => this.logger.debug("UNKOWN MESSAGE FROM SERVER: " + message));
        this.connection.on("error", (message) => { this.failReason = message; this.connectionReject(message); });
    }
    resetConnections(videoElement, audioElement, iceServers) {
        this.failReason = null;
        let resolveFn;
        let rejectFn;
        this.connectionPromise = new Promise((resolve, reject) => {
            resolveFn = resolve;
            rejectFn = reject;
        });
        this.connectionResolve = resolveFn;
        this.connectionReject = rejectFn;
        this.connectionTimeout = setTimeout(() => this.connectionReject("Connection Timed Out"), this.CONNECTION_TIMEOUT_MS);
        switch (this.transport) {
            case "livekit":
                this.connection = new LivekitTransport_1.LivekitTransport(this.SimliWSURL, this.session_token, videoElement, audioElement, this.logger, this.connectionReject);
                break;
            case "p2p":
                if (!iceServers || iceServers.length == 0) {
                    throw "Ice Servers Required for P2P Mode";
                }
                this.connection = new P2PTransport_1.P2PTransport(this.SimliWSURL, this.session_token, true, iceServers, videoElement, audioElement, this.logger, this.connectionReject);
                break;
            default:
                throw new Error("Not Implemented Yet");
        }
        this.connection.on("start", () => {
            this.connectionResolve();
            clearTimeout(this.connectionTimeout);
        });
        this.connection.on("error", (message) => { this.retryAttempt = this.MAX_RETRY_ATTEMPTS; this.connectionReject(message); });
        this.connection.on("unknown", (message) => this.logger.debug("UNKOWN MESSAGE FROM SERVER: " + message));
        // Re-register all user event handlers on the new connection
        this.persistent_events.forEach((callbacks, event) => {
            callbacks.forEach((callback) => {
                this.connection.on(event, callback);
            });
        });
    }
    async start() {
        if (this.shouldStop) {
            throw new Error("Disconnect Already Called, Can't reuse same SimliClient multiple times create a new SimliClient Object");
        }
        try {
            await this.connection.connect();
            await this.connectionPromise;
            this.retryAttempt = 0;
        }
        catch (error) {
            if (this.failReason) {
                throw error;
            }
            if (this.retryAttempt >= this.MAX_RETRY_ATTEMPTS)
                throw new Error("Too Many Retry Attempts Failed to connect");
            if (this.shouldStop) {
                this.shouldStop = false;
                throw new Error("Called Disconnect Before A Connecction succeeded");
            }
            this.logger.error("FAILED: " + error);
            await this.connection.disconnect();
            await new Promise(resolve => setTimeout(resolve, this.RETRY_DELAY));
            this.retryAttempt += 1;
            if (this.retryAttempt > 2)
                this.transport = "livekit";
            this.resetConnections(this.videoElement, this.audioElement, this.iceServers);
            await this.start();
        }
    }
    async stop() {
        this.shouldStop = true;
        await this.connection.disconnect();
    }
    listenToMediastreamTrack(stream) {
        const source = this.audioContext.createMediaStreamSource(new MediaStream([stream]));
        this.sourceNode = source;
        this.attachSourceToWorklet(this.audioContext, source);
    }
    listenToAudioElement(audioEl) {
        const source = this.audioContext.createMediaElementSource(audioEl);
        // No connection to audioContext.destination on purpose: createMediaElementSource
        // reroutes the element's audio into the graph, so leaving the destination
        // unconnected silences the <audio> while samples still flow to Simli.
        this.attachSourceToWorklet(this.audioContext, source);
    }
    attachSourceToWorklet(audioContext, source) {
        audioContext.audioWorklet
            .addModule(URL.createObjectURL(new Blob([AudioProcessor(this.audioBufferSize)], {
            type: "application/javascript",
        })))
            .then(() => {
            this.audioWorklet = new AudioWorkletNode(audioContext, "audio-processor");
            if (this.audioWorklet === null) {
                throw new Error("SIMLI: AudioWorklet not initialized");
            }
            source.connect(this.audioWorklet);
            this.audioWorklet.port.onmessage = (event) => {
                if (event.data.type === "audioData") {
                    this.connection.signalingConnection.sendAudioData(new Uint8Array(event.data.data.buffer));
                }
            };
        });
    }
    ClearBuffer = () => {
        this.connection.signalingConnection.sendSignal("SKIP");
    };
    sendAudioData(audioData) {
        this.connection.signalingConnection.sendAudioData(audioData);
    }
    sendAudioDataImmediate(audioData) {
        this.connection.signalingConnection.sendAudioDataImmediate(audioData);
    }
}
exports.SimliClient = SimliClient;

};
M["config.js"]=function(module,exports,require){
"use strict";
Object.defineProperty(exports, "__esModule", { value: true });

};
M["Events.js"]=function(module,exports,require){
"use strict";
Object.defineProperty(exports, "__esModule", { value: true });

};
M["Logger.js"]=function(module,exports,require){
"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.Logger = exports.LogLevel = void 0;
var LogLevel;
(function (LogLevel) {
    LogLevel[LogLevel["DEBUG"] = 0] = "DEBUG";
    LogLevel[LogLevel["INFO"] = 1] = "INFO";
    LogLevel[LogLevel["ERROR"] = 2] = "ERROR";
    LogLevel[LogLevel["CRITICAL"] = 3] = "CRITICAL";
})(LogLevel || (exports.LogLevel = LogLevel = {}));
class Logger {
    currentLevel;
    destination;
    session_id;
    constructor(level = LogLevel.INFO) {
        this.currentLevel = level;
        this.destination = null;
        this.session_id = null;
    }
    formatMessage(level, message) {
        const timestamp = new Date().toISOString();
        const destination = this.destination ?? 'not_received';
        const sessionId = this.session_id ?? 'not_received';
        return `SimliClient | ${timestamp} | ${level} | ${destination}/${sessionId} | ${message}`;
    }
    log(level, levelName, message, ...args) {
        if (level < this.currentLevel) {
            return;
        }
        const formattedMessage = this.formatMessage(levelName, message);
        switch (level) {
            case LogLevel.DEBUG:
            case LogLevel.INFO:
                console.log(formattedMessage, ...args);
                break;
            case LogLevel.ERROR:
            case LogLevel.CRITICAL:
                console.error(formattedMessage, ...args);
                break;
        }
    }
    debug(message, ...args) {
        this.log(LogLevel.DEBUG, 'DEBUG', message, ...args);
    }
    info(message, ...args) {
        this.log(LogLevel.INFO, 'INFO', message, ...args);
    }
    error(message, ...args) {
        this.log(LogLevel.ERROR, 'ERROR', message, ...args);
    }
    critical(message, ...args) {
        this.log(LogLevel.CRITICAL, 'CRITICAL', message, ...args);
    }
    setLevel(level) {
        this.currentLevel = level;
    }
    getLevel() {
        return this.currentLevel;
    }
}
exports.Logger = Logger;

};
M["Signaling/BaseSignaling.js"]=function(module,exports,require){
"use strict";
Object.defineProperty(exports, "__esModule", { value: true });

};
M["Signaling/WebSocketSignaling.js"]=function(module,exports,require){
"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.WebSocketSignaling = void 0;
const Logger_1 = require("../Logger");
class WebSocketSignaling {
    wsURL;
    wsConnection;
    logger;
    constructor(wsURL, logger) {
        this.wsURL = wsURL;
        this.wsConnection = new WebSocket(this.wsURL);
        this.wsConnection.addEventListener("message", (message) => (this.logger.debug(message.data)));
        this.logger = logger;
    }
    async connect(connected) {
        this.wsConnection.onopen = connected;
    }
    disconnect() {
        this.wsConnection.close();
    }
    send(data) {
        if (this.wsConnection.readyState != WebSocket.OPEN) {
            throw `Invalid State, WS Connection ${this.wsConnection.readyState.toString()}`;
        }
        this.wsConnection.send(data);
    }
    sendOffer(offer) {
        this.send(JSON.stringify(offer));
    }
    sendSignal(data) {
        this.send(data);
    }
    sendAudioData(audioData) {
        if (this.logger.getLevel() === Logger_1.LogLevel.DEBUG)
            this.logger.debug("Sent Audio of length: " + (audioData.length / 32000).toString());
        this.send(audioData);
    }
    sendAudioDataImmediate(audioData) {
        if (this.logger.getLevel() === Logger_1.LogLevel.DEBUG)
            this.logger.debug("Sent Audio of length for immediate playback: " + (audioData.length / 32000).toString());
        const asciiStr = "PLAY_IMMEDIATE";
        const encoder = new TextEncoder(); // Default is utf-8
        const strBytes = encoder.encode(asciiStr); // Uint8Array of " World!"
        const buffer = new Uint8Array(strBytes.length + audioData.length);
        buffer.set(strBytes, 0);
        buffer.set(audioData, strBytes.length);
        this.send(buffer);
    }
}
exports.WebSocketSignaling = WebSocketSignaling;

};
M["Transports/BaseTransport.js"]=function(module,exports,require){
"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.handleMessage = handleMessage;
exports.register_destination = register_destination;
function register_destination(logger, serialized_info) {
    const parsed = JSON.parse(serialized_info);
    logger.destination = parsed.destination;
    logger.session_id = parsed.session_id;
}
async function handleMessage(transport, message) {
    const firstToken = message.data.toUpperCase().split(" ")[0];
    switch (firstToken) {
        case "START": {
            // SOFT IGNORE
            break;
        }
        case "ACK": {
            transport.emit("ack");
            break;
        }
        case "STOP": {
            transport.disconnect();
            transport.emit("stop");
            break;
        }
        case "CLOSING":
        case "RATE":
        case "ERROR":
        case "ERROR:": {
            transport.disconnect();
            transport.emit("error", message.data);
        }
        case "SPEAK": {
            transport.emit("speaking");
            break;
        }
        case "SILENT": {
            transport.emit("silent");
            break;
        }
        default: {
            if (firstToken.includes("SDP") || firstToken.includes("LIVEKIT")) {
                transport.emit("connection_info", message.data);
            }
            else if (firstToken.includes("VIDEO_METADATA")) {
                transport.emit("video_info", message.data);
            }
            else if (firstToken.includes("ENDFRAME")) {
                transport.emit("stop");
                transport.disconnect();
            }
            else if (firstToken.includes("DESTINATION")) {
                transport.emit("destination", message.data);
            }
            else {
                transport.emit("unknown", message.data);
            }
        }
    }
}

};
M["Transports/LivekitTransport.js"]=function(module,exports,require){
"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.LivekitTransport = void 0;
const livekit_client_1 = require("livekit-client");
const WebSocketSignaling_1 = require("../Signaling/WebSocketSignaling");
const BaseTransport_1 = require("./BaseTransport");
class LivekitTransport {
    videoElementAnchor;
    audioElementAnchor;
    signalingConnection;
    session_token;
    pc;
    logger;
    events = new Map();
    websocketPromise;
    websocketReject = null;
    constructor(simliBaseWSURL, session_token, videoElementAnchor, audioElementAnchor, logger, failSignal) {
        this.logger = logger;
        this.on("startup_error", failSignal);
        this.session_token = session_token;
        const wsURL = new URL(simliBaseWSURL + "/compose/webrtc/livekit");
        wsURL.searchParams.set("session_token", session_token);
        this.signalingConnection = new WebSocketSignaling_1.WebSocketSignaling(wsURL, this.logger);
        this.on("destination", (serilized_info) => (0, BaseTransport_1.register_destination)(this.logger, serilized_info));
        this.websocketPromise = new Promise((resolve, reject) => {
            this.websocketReject = reject;
            this.signalingConnection.connect(() => {
                resolve("success");
                this.logger.debug("LK WebSocket Connected");
            });
        });
        this.signalingConnection.wsConnection.onmessage = (message) => { (0, BaseTransport_1.handleMessage)(this, message); };
        this.signalingConnection.wsConnection.onerror = (evt) => {
            this.emit("startup_error", "Websocket Failed");
            if (this.websocketReject) {
                this.websocketReject("Websocket Failed");
                this.websocketReject = null; // Prevent multiple rejections
            }
        };
        const options = { adaptiveStream: false, dynacast: true };
        this.pc = new livekit_client_1.Room(options);
        this.on("connection_info", (serialized_info) => this.join_lk_room(serialized_info));
        this.videoElementAnchor = videoElementAnchor;
        this.audioElementAnchor = audioElementAnchor;
    }
    on(event, callback) {
        if (!this.events.has(event)) {
            this.events.set(event, new Set());
        }
        this.events.get(event)?.add(callback);
        this.logger.debug("Registered Callback for Event: " + event);
    }
    off(event, callback) {
        if (!this.events.has(event)) {
            throw "Event Not Regsitered";
        }
        this.events.get(event)?.delete(callback);
    }
    emit(event, ...args) {
        this.logger.debug("Event: " + event);
        this.events.get(event)?.forEach((callback) => {
            callback(...args);
        });
    }
    async connect() {
        this.logger.info("Connecting");
        this.setupConnectionStateHandler();
        await this.websocketPromise;
    }
    async disconnect() {
        this.logger.info("Disconnecting");
        try {
            this.signalingConnection.sendSignal("DONE");
        }
        catch {
            this.logger.error("FAILED TO SEND FINAL MESSAGE");
        }
        try {
            this.signalingConnection.disconnect();
        }
        catch {
            this.logger.error("SIGNALING ALREADY DISCONNECTED");
        }
        try {
            await this.pc.disconnect();
        }
        catch {
            this.logger.error("LOCAL PEER ALREADY CLOSED");
        }
    }
    async join_lk_room(serialized_info) {
        const info = JSON.parse(serialized_info);
        this.logger.debug(info);
        if (info.livekit_url && info.livekit_token) {
            await this.pc.connect(info.livekit_url, info.livekit_token);
        }
        else {
            this.disconnect();
            this.emit("error", "Invalid Join Info, Contact Simli For Support");
        }
    }
    setupConnectionStateHandler() {
        this.pc.on(livekit_client_1.RoomEvent.Disconnected, () => {
            this.disconnect();
        });
        this.pc.on(livekit_client_1.RoomEvent.Connected, () => {
        });
        this.pc.on(livekit_client_1.RoomEvent.TrackSubscribed, (track, publication, participant) => {
            this.logger.debug("Track Received: " + track.kind);
            if (track.kind === livekit_client_1.Track.Kind.Video) {
                track.attach(this.videoElementAnchor);
                this.videoElementAnchor.requestVideoFrameCallback(() => {
                    this.emit("start");
                });
            }
            else if (track.kind === livekit_client_1.Track.Kind.Audio) {
                track.attach(this.audioElementAnchor);
            }
        });
    }
    ;
}
exports.LivekitTransport = LivekitTransport;

};
M["Transports/P2PTransport.js"]=function(module,exports,require){
"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.P2PTransport = void 0;
const WebSocketSignaling_1 = require("../Signaling/WebSocketSignaling");
const BaseTransport_1 = require("./BaseTransport");
class P2PTransport {
    videoElementAnchor;
    audioElementAnchor;
    signalingConnection;
    session_token;
    pc;
    events = new Map();
    logger;
    iceCandidateCount;
    previousIceCandidateCount;
    iceTimeout = null;
    websocketPromise;
    websocketReject = null;
    constructor(simliBaseWSURL, session_token, enableSFU, iceServers, videoElementAnchor, audioElementAnchor, logger, failSignal) {
        this.logger = logger;
        this.on("startup_error", failSignal);
        this.session_token = session_token;
        const wsURL = new URL(simliBaseWSURL + "/compose/webrtc/p2p");
        wsURL.searchParams.set("session_token", session_token);
        wsURL.searchParams.set("enableSFU", String(enableSFU));
        this.on("destination", (serilized_info) => (0, BaseTransport_1.register_destination)(this.logger, serilized_info));
        this.signalingConnection = new WebSocketSignaling_1.WebSocketSignaling(wsURL, this.logger);
        this.websocketPromise = new Promise((resolve, reject) => {
            this.websocketReject = reject;
            this.signalingConnection.connect(() => {
                resolve("success");
                this.logger.debug("P2P WebSocket Connected");
            });
        });
        this.signalingConnection.wsConnection.onmessage = (message) => { (0, BaseTransport_1.handleMessage)(this, message); };
        this.signalingConnection.wsConnection.onerror = (evt) => {
            this.emit("startup_error", "Websocket Failed");
            if (this.websocketReject) {
                this.websocketReject("Websocket Failed");
                this.websocketReject = null; // Prevent multiple rejections
            }
        };
        this.on("connection_info", (serialized_info) => this.registerPeerInfo(serialized_info));
        this.videoElementAnchor = videoElementAnchor;
        this.audioElementAnchor = audioElementAnchor;
        this.iceCandidateCount = 0;
        this.previousIceCandidateCount = 0;
        const config = {
            sdpSemantics: "unified-plan",
            iceServers: iceServers,
        };
        this.pc = new window.RTCPeerConnection(config);
        this.pc.addTransceiver("audio", {
            direction: "recvonly",
        });
        this.pc.addTransceiver("video", {
            direction: "recvonly",
        });
    }
    on(event, callback) {
        if (!this.events.has(event)) {
            this.events.set(event, new Set());
        }
        this.events.get(event)?.add(callback);
    }
    off(event, callback) {
        this.events.get(event)?.delete(callback);
    }
    emit(event, ...args) {
        this.events.get(event)?.forEach((callback) => {
            try {
                callback(...args);
            }
            catch {
                this.logger.error("CALLBACK FAILED: " + callback.name);
            }
        });
    }
    async connect() {
        const offer = await this.pc.createOffer();
        await this.pc.setLocalDescription(offer);
        await this.waitForIceGathering();
        this.setupPeerConnectionListeners();
        await this.websocketPromise;
        if (this.pc.localDescription) {
            this.signalingConnection.sendOffer(this.pc.localDescription);
        }
    }
    async disconnect() {
        this.logger.info("Disconnecting");
        try {
            this.signalingConnection.sendSignal("DONE");
        }
        catch {
            this.logger.error("FAILED TO SEND FINAL MESSAGE");
        }
        try {
            this.signalingConnection.disconnect();
        }
        catch {
            this.logger.error("SIGNALING ALREADY DISCONNECTED");
        }
        try {
            this.pc.close();
        }
        catch {
            this.logger.error("LOCAL PEER ALREADY CLOSED");
        }
    }
    async registerPeerInfo(serialized_info) {
        const info = JSON.parse(serialized_info);
        if (info.sdp && info.type == "answer") {
            await this.pc.setRemoteDescription(new RTCSessionDescription(info));
        }
        else {
            this.disconnect();
            this.emit("error", "Invalid Join Info, Contact Simli For Support");
        }
    }
    async waitForIceGathering() {
        this.iceCandidateCount = 0;
        this.previousIceCandidateCount = 0;
        if (this.pc.iceGatheringState === "complete") {
            return;
        }
        return new Promise((resolve, reject) => {
            if (!this.iceTimeout) {
                this.iceTimeout = setTimeout(() => {
                    reject(new Error("ICE gathering timeout"));
                }, 10000);
            }
            const checkIceCandidates = () => {
                if (this.pc.iceGatheringState === "complete" ||
                    this.iceCandidateCount === this.previousIceCandidateCount) {
                    if (this.iceTimeout) {
                        clearTimeout(this.iceTimeout);
                    }
                    resolve();
                }
                else {
                    this.previousIceCandidateCount = this.iceCandidateCount;
                    setTimeout(checkIceCandidates, 150);
                }
            };
            checkIceCandidates();
        });
    }
    setupPeerConnectionListeners() {
        this.pc.addEventListener("track", (evt) => {
            if (evt.track.kind === "video") {
                this.videoElementAnchor.srcObject = evt.streams[0];
                this.videoElementAnchor.requestVideoFrameCallback(() => {
                    this.emit("start");
                });
            }
            else if (evt.track.kind === "audio" && this.audioElementAnchor) {
                this.audioElementAnchor.srcObject = evt.streams[0];
            }
        });
        this.pc.onicecandidate = (event) => {
            if (event.candidate !== null) {
                this.iceCandidateCount += 1;
            }
        };
    }
}
exports.P2PTransport = P2PTransport;

};
window.Simli=R("_")("./index.js");})();
