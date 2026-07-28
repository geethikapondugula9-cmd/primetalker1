// voice-processor.js - Robust Live Translation with Sentence Accumulation
const fs = require("fs");
const path = require("path");
const speech = require("@google-cloud/speech");
const textToSpeech = require("@google-cloud/text-to-speech");
const { Translate } = require("@google-cloud/translate").v2;

function loadGoogleCredentials() {
    const candidates = [];

    if (process.env.GOOGLE_CREDENTIALS) {
        candidates.push({ source: "GOOGLE_CREDENTIALS", value: process.env.GOOGLE_CREDENTIALS });
    }

    if (process.env.GOOGLE_APPLICATION_CREDENTIALS) {
        candidates.push({ source: "GOOGLE_APPLICATION_CREDENTIALS", value: process.env.GOOGLE_APPLICATION_CREDENTIALS });
    }

    const defaultCredentialPath = path.resolve(__dirname, "google-credentials.json");
    candidates.push({ source: "default file", value: defaultCredentialPath });

    for (const candidate of candidates) {
        if (!candidate.value) continue;

        if (typeof candidate.value === "string" && candidate.value.trim().startsWith("{")) {
            try {
                const parsed = JSON.parse(candidate.value);
                if (parsed && parsed.client_email) {
                    console.log(`✅ Google credentials loaded from ${candidate.source}`);
                    return parsed;
                }
            } catch (e) {
                console.warn(`⚠️ Failed to parse Google credentials from ${candidate.source}:`, e.message);
            }
        }

        const resolvedPath = path.isAbsolute(candidate.value)
            ? candidate.value
            : path.resolve(__dirname, candidate.value);

        if (fs.existsSync(resolvedPath)) {
            try {
                const fileContent = fs.readFileSync(resolvedPath, "utf8");
                const parsed = JSON.parse(fileContent);
                if (parsed && parsed.client_email) {
                    process.env.GOOGLE_APPLICATION_CREDENTIALS = resolvedPath;
                    console.log(`✅ Google credentials loaded from ${resolvedPath}`);
                    return parsed;
                }
            } catch (e) {
                console.warn(`⚠️ Failed to load Google credentials from ${resolvedPath}:`, e.message);
            }
        }
    }

    console.warn("⚠️ No valid Google credentials found. Set GOOGLE_CREDENTIALS or GOOGLE_APPLICATION_CREDENTIALS.");
    return null;
}

let googleCredentials = loadGoogleCredentials();

class VoiceProcessor {
    constructor(websocket, activeSessions) {
        this.ws = websocket;
        this.activeSessions = activeSessions;

        // Google Cloud clients (use credentials from file or environment)
        const clientConfig = googleCredentials
            ? {
                credentials: googleCredentials,
                projectId: googleCredentials.project_id || undefined
            }
            : {};

        this.speechClient = new speech.SpeechClient(clientConfig);
        this.ttsClient = new textToSpeech.TextToSpeechClient(clientConfig);
        this.translateClient = new Translate(googleCredentials ? { credentials: googleCredentials } : {});

        // User info
        this.roomId = null;
        this.userType = null;
        this.myLanguage = null;
        this.myName = null;
        this.myVoice = "male";

        // STT state
        this.recognizeStream = null;
        this.isStreaming = false;
        this.isStartingStream = false; // Prevents multiple parallel start attempts
        this.streamCreatedAt = 0;
        this.lastAudioTime = 0;        // Track last audio received (for idle detection)
        this.audioBuffer = [];         // Buffers audio while connection is opening

        // Sentence building - THE KEY FIX
        this.sentence = "";           // Current accumulated sentence (from finals)
        this.lastInterim = "";        // Backup: latest interim result
        this.lastSentence = "";       // Last processed sentence
        this.sentenceTimer = null;    // Timer to finalize sentence
        this.SENTENCE_TIMEOUT = 1500; // 1.5s of silence = end of sentence

        // Deduplication: track last 5 processed sentences to prevent any repeat
        this._recentSentences = [];
        this.MAX_RECENT = 5;

        // Processing lock
        this.isProcessing = false;

        // Caches for translation & TTS (avoid redundant API calls)
        this.translationCache = new Map(); // key: "text|from|to" → translated text
        this.ttsCache = new Map();         // key: "text|lang" → audio buffer
        this.MAX_TRANSLATION_CACHE = 100;
        this.MAX_TTS_CACHE = 50;

        // Bind handlers
        this._handleSTTData = this._handleSTTData.bind(this);
        this._handleSTTError = this._handleSTTError.bind(this);
    }

    async handleMessage(msg) {
        switch (msg.event) {
            case "connected":
                this.roomId = msg.roomId;
                this.userType = msg.userType;
                this.myLanguage = msg.myLanguage;
                this.myVoice = msg.myVoice || "male";
                this.myName = msg.myName || "User";
                console.log(`✅ ${this.userType} connected in ${this.roomId} (${this.myLanguage})`);
                this._registerConnection();
                this._sendChatHistory();
                this._notifyPartner("user_joined", { name: this.myName, language: this.myLanguage });

                // Pre-warm STT stream IMMEDIATELY
                if (!this.isStreaming && !this.isStartingStream) {
                    console.log(`🔥 Pre-warming STT stream for ${this.myLanguage}...`);
                    this._startStream().then(() => {
                        if (this.recognizeStream) {
                            const silence = Buffer.alloc(3200); // 100ms at 16kHz mono 16-bit
                            try { this.recognizeStream.write(silence); } catch (e) { }
                            console.log(`🔥 Warm-up audio sent for ${this.myLanguage}`);
                        }
                    });
                }
                break;
            case "audio":
                this._processAudio(msg.audio);
                break;
            case "chat:send":
                console.log("📨 Chat received:", msg);
                await this._handleChatMessage(msg);
                break;
            case "disconnect":
            case "stop":
                await this.cleanup();
                break;
        }
    }

    _registerConnection() {
        const session = this.activeSessions.get(this.roomId);
        if (!session) return;
        if (this.userType === "caller") session.callerConnection = this;
        else session.receiverConnection = this;
    }

    async _processAudio(base64Audio) {
        if (!this.myLanguage) return;

        this.lastAudioTime = Date.now(); // Track when we last received audio
        const buffer = Buffer.from(base64Audio, "base64");
        // === BYPASS: same language, skip STT/Translate/TTS entirely ===
        const partner = this._getPartner();
        if (partner && this._isSameLanguage(partner)) {
            if (partner.ws?.readyState === 1) {
                partner.ws.send(JSON.stringify({
                    event: "audio_direct",
                    audio: base64Audio
                }));
            }
            return; // never touches gRPC/STT — this is where the cost/latency savings come from
        }


        // If currently starting, buffer the audio so we don't lose the first words
        if (this.isStartingStream) {
            this.audioBuffer.push(buffer);
            if (this.audioBuffer.length > 100) this.audioBuffer.shift();
            return;
        }

        // Ensure stream is running (continuous - always restart if down)
        if (!this.isStreaming) {
            this._startStream();
            this.audioBuffer.push(buffer);
            return;
        }

        // Proactively restart before Google's 60s limit (seamless)
        const streamAge = Date.now() - this.streamCreatedAt;
        if (streamAge > 50000) {
            console.log("🔄 Seamless stream restart (age limit)");
            this._restartStream();
            this.audioBuffer.push(buffer);
            return;
        }

        // Schedule idle shutdown: if no audio for 25s, end the stream to avoid 408 timeouts
        if (this._idleTimer) clearTimeout(this._idleTimer);
        this._idleTimer = setTimeout(() => {
            if (this.isStreaming) {
                this._stopStream();
            }
        }, 25000);

        // Send audio to Google
        if (this.recognizeStream) {
            try {
                this.recognizeStream.write(buffer);
            } catch (e) {
                console.error("Write error:", e.message);
                this._restartStream();
                this.audioBuffer.push(buffer);
            }
        }
    }

    async _startStream() {
        if (this.isStreaming || this.isStartingStream) return;
        this.isStartingStream = true;

        const langCode = this._getLangCode(this.myLanguage);

        try {
            this.recognizeStream = this.speechClient
                .streamingRecognize({
                    config: {
                        encoding: "LINEAR16",
                        sampleRateHertz: 16000,
                        languageCode: langCode,
                        enableAutomaticPunctuation: true,
                        model: "latest_long"
                    },
                    interimResults: true,
                    singleUtterance: false
                })
                .on("data", this._handleSTTData)
                .on("error", this._handleSTTError)
                .on("end", () => {
                    this.isStreaming = false;
                    this.recognizeStream = null;
                    // Only restart if audio received in last 20s (prevent idle 408s)
                    const idleMs = Date.now() - this.lastAudioTime;
                    if (this.myLanguage && this.ws?.readyState === 1 && idleMs < 20000) {
                        this._startStream();
                    }
                });

            this.isStreaming = true;
            this.isStartingStream = false;
            this.streamCreatedAt = Date.now();
            console.log(`🎤 Stream started: ${langCode}`);

            // Replay any audio that arrived while we were starting
            if (this.audioBuffer.length > 0) {
                const chunks = this.audioBuffer.splice(0);
                chunks.forEach(chunk => {
                    try { this.recognizeStream.write(chunk); } catch (e) { }
                });
                console.log(`📡 Replayed ${chunks.length} buffered chunks`);
            }
        } catch (e) {
            console.error("Failed to start stream:", e.message);
            this.isStreaming = false;
            this.isStartingStream = false;
        }
    }

    async _stopStream() {
        if (this.recognizeStream) {
            try { this.recognizeStream.end(); } catch (e) { }
        }
        this.recognizeStream = null;
        this.isStreaming = false;
    }

    async _restartStream() {
        // Only save in-progress finals (not interim) — saving interim causes sentences to compound
        const savedSentence = this.sentence;
        await this._stopStream();
        this.sentence = savedSentence;
        // lastInterim is intentionally NOT restored — it resets per sentence
        await this._startStream();
    }

    _handleSTTData(response) {
        if (!response.results?.[0]) return;

        const result = response.results[0];
        const rawTranscript = result.alternatives?.[0]?.transcript?.trim();
        if (!rawTranscript) return;

        const isFinal = result.isFinal;

        // Strip already-processed sentence prefix from cumulative STT results.
        // Google STT returns growing cumulative text within one session:
        //   "Hello" → "Hello how are you" → "Hello how are you I am fine"
        // After sentence 1 ("Hello how are you") is finalized, sentence 2's STT
        // result still contains sentence 1 as a prefix. Strip it so only NEW
        // words are accumulated.
        let transcript = rawTranscript;
        if (this.lastSentence && transcript.startsWith(this.lastSentence)) {
            transcript = transcript.slice(this.lastSentence.length).trim();
        }

        if (!transcript) return; // nothing new to add

        if (isFinal) {
            // Accumulate final results into the sentence
            this.sentence = this.sentence ? this.sentence + " " + transcript : transcript;
            this.lastInterim = ""; // Clear interim since we got final
            console.log(`📝 Final: "${this.sentence}"`);
        } else {
            // Save interim as backup (critical for regional languages)
            const preview = this.sentence ? this.sentence + " " + transcript : transcript;
            this.lastInterim = preview;
            this._sendToUI({ event: "transcript_interim", text: preview });
        }

        // Reset timer - user is still speaking
        this._resetSentenceTimer();
    }

    _handleSTTError(err) {
        const msg = err.message || "";
        const isExpectedTimeout =
            msg.includes("Audio Timeout") ||
            msg.includes("OUT_OF_RANGE") ||
            msg.includes("408") ||
            msg.includes("Request Timeout") ||
            err.code === 11;

        if (!isExpectedTimeout) {
            console.error("❌ STT Error:", err.code, msg);
        }

        this.isStreaming = false;
        this.recognizeStream = null;

        // Move interim to sentence BEFORE calling finalize (prevents double-use)
        if (!this.sentence && this.lastInterim) {
            console.log(`🔄 Using interim backup: "${this.lastInterim}"`);
            this.sentence = this.lastInterim;
        }
        this.lastInterim = ""; // clear now so _finalizeSentence can't re-use it

        // Process any accumulated sentence (dedup handled inside _finalizeSentence)
        if (this.sentence) {
            this._finalizeSentence();
        }

        // Continuous: immediately restart stream
        if (this.myLanguage && this.ws?.readyState === 1) {
            this._startStream();
        }
    }

    _resetSentenceTimer() {
        if (this.sentenceTimer) {
            clearTimeout(this.sentenceTimer);
        }
        this.sentenceTimer = setTimeout(() => {
            this._finalizeSentence();
        }, this.SENTENCE_TIMEOUT);
    }

    _finalizeSentence() {
        if (this.sentenceTimer) {
            clearTimeout(this.sentenceTimer);
            this.sentenceTimer = null;
        }

        // Use interim text as backup if no finals were accumulated
        // (very common for regional/Indian languages where Google sends mostly interims)
        if (!this.sentence && this.lastInterim) {
            console.log(`🔄 Using interim as sentence: "${this.lastInterim}"`);
            this.sentence = this.lastInterim;
            this.lastInterim = "";
        }

        const finalSentence = (this.sentence || "").trim();
        this.sentence = "";
        this.lastInterim = "";

        if (!finalSentence) return;

        // Strong dedup: reject if this exact sentence was already processed recently
        if (this._recentSentences.includes(finalSentence)) {
            console.log(`⏩ Skipping duplicate: "${finalSentence}"`);
            return;
        }

        console.log(`\n🔵 SENTENCE COMPLETE: "${finalSentence}"\n`);

        // Track in recent history (rolling window)
        this._recentSentences.push(finalSentence);
        if (this._recentSentences.length > this.MAX_RECENT) {
            this._recentSentences.shift();
        }
        this.lastSentence = finalSentence;

        // Translate and speak (queued, never dropped)
        this._queueTranslation(finalSentence);

        // Cleanly reset Google STT context to prevent word repetition
        if (this.isStreaming) {
            this._restartStream();
        }
    }

    _queueTranslation(text) {
        if (!this._translationQueue) this._translationQueue = [];
        this._translationQueue.push(text);
        if (!this._isTranslating) {
            this._processTranslationQueue();
        }
    }

    async _processTranslationQueue() {
        if (this._isTranslating) return;
        this._isTranslating = true;

        while (this._translationQueue && this._translationQueue.length > 0) {
            const text = this._translationQueue.shift();
            await this._translateAndSpeak(text);
        }

        this._isTranslating = false;
    }

    async _translateAndSpeak(text) {
        if (!text) return;
        const start = Date.now();

        try {
            const session = this.activeSessions.get(this.roomId);
            if (!session) return;

            const partner = this.userType === "caller"
                ? session.receiverConnection
                : session.callerConnection;

            if (!partner?.myLanguage) {
                console.log("⚠️ Partner not connected");
                return;
            }

            // Step 1: Translate only if languages are different
            // Step 1: Check if translation is needed
            const fromLang = (this.myLanguage || "en").split("-")[0];
            const toLang = (partner.myLanguage || "en").split("-")[0];

            const t0 = Date.now();
            let translated;

            if (fromLang === toLang) {
                console.log(`✅ Same language (${fromLang}) detected. Skipping translation.`);
                translated = text;
            } else {
                translated = await this._translate(text, fromLang, toLang);
            }

            const translateMs = Date.now() - t0;

            // Send translation text to both users immediately (don't wait for TTS)
            const data = {
                event: "translation",
                originalText: text,
                translatedText: translated,
                fromUser: this.userType,
                fromLanguage: this.myLanguage,
                toLanguage: partner.myLanguage,
                translationSkipped: fromLang === toLang
            };
            this._sendToUI(data);
            partner._sendToUI(data);

            // Step 2: Generate TTS (with cache)
            const t1 = Date.now();
            const audio = await this._tts(translated, partner.myLanguage);
            const ttsMs = Date.now() - t1;

            if (audio && partner.ws?.readyState === 1) {
                const wav = this._toWav(audio, 48000);
                partner.ws.send(JSON.stringify({
                    event: "audio_playback",
                    audio: wav.toString("base64"),
                    format: "wav"
                }));
            }

            const totalMs = Date.now() - start;
            console.log(`⏱️ ${translateMs}ms+${ttsMs}ms=${totalMs}ms | "${text}" → "${translated}"`);
        } catch (e) {
            console.error("Translation error:", e.message);
        }
    }

    async _handleChatMessage(payload) {
        if (!payload?.message) return;

        const session = this.activeSessions.get(this.roomId);
        if (!session) return;

        const messageId = payload.messageId || `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
        const senderId = payload.senderId || `${this.roomId}-${this.userType}`;
        const senderName = payload.senderName || this.myName || "User";
        const originalMessage = payload.message;
        const sourceLanguage = payload.senderLanguage || this.myLanguage || "en";

        const senderMessage = {
            id: messageId,
            senderId,
            senderName,
            originalMessage,
            translatedMessage: originalMessage,
            sourceLanguage,
            targetLanguage: sourceLanguage,
            timestamp: Date.now(),
            isLocal: true,
        };

        const sessionMessages = session.chatMessages || [];
        sessionMessages.push(senderMessage);
        if (sessionMessages.length > 100) {
            sessionMessages.splice(0, sessionMessages.length - 100);
        }
        session.chatMessages = sessionMessages;

        this._sendToUI({ event: "chat:receive", message: senderMessage });

        const partner = this.userType === "caller"
            ? session.receiverConnection
            : session.callerConnection;
        console.log("My language:", this.myLanguage);
        console.log("Partner language:", partner?.myLanguage);
        if (!partner?.myLanguage) return;
        console.log("Source Language:", sourceLanguage);
        console.log("Target Language:", partner.myLanguage);
        console.log("Same Language:", sourceLanguage === partner.myLanguage);

        const translatedMessage = await this._translate(originalMessage, sourceLanguage, partner.myLanguage);
        const partnerMessage = {
            ...senderMessage,
            translatedMessage,
            targetLanguage: partner.myLanguage,
            isLocal: false,
        };

        partner._sendToUI({ event: "chat:receive", message: partnerMessage });
    }

    async _translate(text, from, to) {
        const fromLang = (from || "en").split("-")[0];
        const toLang = (to || "en").split("-")[0];
        if (fromLang === toLang) return text;

        // Check cache first
        const cacheKey = `${text}|${fromLang}|${toLang}`;
        if (this.translationCache.has(cacheKey)) {
            console.log(`💾 Translation cache hit`);
            return this.translationCache.get(cacheKey);
        }

        try {
            console.log("Translating:", {
                text,
                from,
                to
            });
            const [result] = await this.translateClient.translate(text, { from: fromLang, to: toLang });

            // Store in cache (evict oldest if full)
            if (this.translationCache.size >= this.MAX_TRANSLATION_CACHE) {
                const oldest = this.translationCache.keys().next().value;
                this.translationCache.delete(oldest);
            }
            this.translationCache.set(cacheKey, result);

            return result;
        } catch (e) {
            console.error("Translate error:", e.message);
            return text;
        }
    }

    async _tts(text, lang) {
        // Comprehensive language support with Neural2 where available
        const voices = {
            // Major World Languages
            en: { languageCode: "en-US", name: "en-US-Neural2-J" },
            es: { languageCode: "es-ES", name: "es-ES-Neural2-A" },
            fr: { languageCode: "fr-FR", name: "fr-FR-Neural2-A" },
            de: { languageCode: "de-DE", name: "de-DE-Neural2-A" },
            pt: { languageCode: "pt-BR", name: "pt-BR-Neural2-A" },
            it: { languageCode: "it-IT", name: "it-IT-Neural2-A" },
            ru: { languageCode: "ru-RU", name: "ru-RU-Standard-A" },

            // Asian Languages
            zh: { languageCode: "cmn-CN", name: "cmn-CN-Standard-A" },
            ja: { languageCode: "ja-JP", name: "ja-JP-Neural2-B" },
            ko: { languageCode: "ko-KR", name: "ko-KR-Neural2-A" },
            vi: { languageCode: "vi-VN", name: "vi-VN-Neural2-A" },
            th: { languageCode: "th-TH", name: "th-TH-Neural2-C" },
            id: { languageCode: "id-ID", name: "id-ID-Standard-A" },
            ms: { languageCode: "ms-MY", name: "ms-MY-Standard-A" },
            fil: { languageCode: "fil-PH", name: "fil-PH-Neural2-A" },

            // Indian Languages (Neural2 for Hindi, Standard for others)
            hi: { languageCode: "hi-IN", name: "hi-IN-Neural2-A" },
            te: { languageCode: "te-IN", name: "te-IN-Standard-A" },
            ta: { languageCode: "ta-IN", name: "ta-IN-Standard-A" },
            bn: { languageCode: "bn-IN", name: "bn-IN-Standard-A" },
            gu: { languageCode: "gu-IN", name: "gu-IN-Standard-A" },
            kn: { languageCode: "kn-IN", name: "kn-IN-Standard-A" },
            ml: { languageCode: "ml-IN", name: "ml-IN-Standard-A" },
            mr: { languageCode: "mr-IN", name: "mr-IN-Standard-A" },
            pa: { languageCode: "pa-IN", name: "pa-IN-Standard-A" },

            // Middle Eastern Languages
            ar: { languageCode: "ar-XA", name: "ar-XA-Standard-A" },
            he: { languageCode: "he-IL", name: "he-IL-Standard-A" },
            tr: { languageCode: "tr-TR", name: "tr-TR-Neural2-A" },
            fa: { languageCode: "fa-IR", name: "fa-IR-Standard-A" },

            // European Languages
            nl: { languageCode: "nl-NL", name: "nl-NL-Neural2-A" },
            pl: { languageCode: "pl-PL", name: "pl-PL-Neural2-A" },
            sv: { languageCode: "sv-SE", name: "sv-SE-Neural2-A" },
            da: { languageCode: "da-DK", name: "da-DK-Neural2-D" },
            no: { languageCode: "nb-NO", name: "nb-NO-Neural2-A" },
            fi: { languageCode: "fi-FI", name: "fi-FI-Neural2-A" },
            el: { languageCode: "el-GR", name: "el-GR-Neural2-A" },
            cs: { languageCode: "cs-CZ", name: "cs-CZ-Standard-A" },
            ro: { languageCode: "ro-RO", name: "ro-RO-Standard-A" },
            hu: { languageCode: "hu-HU", name: "hu-HU-Standard-A" },
            uk: { languageCode: "uk-UA", name: "uk-UA-Standard-A" },

            // Other
            af: { languageCode: "af-ZA", name: "af-ZA-Standard-A" }
        };

        const base = (lang || "en").split("-")[0];
        // const voice = {
        //     ...(voices[base] || { languageCode: lang }),
        //     ssmlGender: this.myVoice === "female" ? "FEMALE" : "MALE"
        // };
        const voice = voices[base] || { languageCode: lang, ssmlGender: "NEUTRAL" };
        // Check TTS cache first
        const ttsCacheKey = `${text}|${base}`;
        if (this.ttsCache.has(ttsCacheKey)) {
            console.log(`💾 TTS cache hit`);
            return this.ttsCache.get(ttsCacheKey);
        }

        try {
            const [response] = await this.ttsClient.synthesizeSpeech({
                input: { text },
                voice,
                audioConfig: { audioEncoding: "LINEAR16", sampleRateHertz: 48000, speakingRate: 1.15 }
            });

            // Store in cache (evict oldest if full)
            if (this.ttsCache.size >= this.MAX_TTS_CACHE) {
                const oldest = this.ttsCache.keys().next().value;
                this.ttsCache.delete(oldest);
            }
            this.ttsCache.set(ttsCacheKey, response.audioContent);

            return response.audioContent;
        } catch (e) {
            console.error("TTS error:", e.message);
            // Fallback: retry with just languageCode + NEUTRAL gender (no specific voice name)
            try {
                console.log(`🔄 TTS fallback for ${base}...`);
                const [fallback] = await this.ttsClient.synthesizeSpeech({
                    input: { text },
                    voice: { languageCode: voice.languageCode || lang, ssmlGender: "NEUTRAL" },
                    audioConfig: { audioEncoding: "LINEAR16", sampleRateHertz: 48000, speakingRate: 1.15 }
                });
                return fallback.audioContent;
            } catch (e2) {
                console.error("TTS fallback error:", e2.message);
                return null;
            }
        }
    }

    _getLangCode(lang) {
        const map = {
            // Major World Languages
            en: "en-US", es: "es-ES", fr: "fr-FR", de: "de-DE", pt: "pt-BR",
            it: "it-IT", ru: "ru-RU", nl: "nl-NL", pl: "pl-PL",

            // Asian Languages
            zh: "cmn-CN", ja: "ja-JP", ko: "ko-KR", vi: "vi-VN",
            th: "th-TH", id: "id-ID", ms: "ms-MY", fil: "fil-PH",

            // Indian Languages (ALL)
            hi: "hi-IN", te: "te-IN", ta: "ta-IN", bn: "bn-IN",
            gu: "gu-IN", kn: "kn-IN", ml: "ml-IN", mr: "mr-IN",
            pa: "pa-IN", ur: "ur-IN",

            // Middle Eastern
            ar: "ar-XA", he: "he-IL", tr: "tr-TR", fa: "fa-IR",

            // European
            sv: "sv-SE", da: "da-DK", no: "nb-NO", fi: "fi-FI",
            el: "el-GR", cs: "cs-CZ", ro: "ro-RO", hu: "hu-HU",
            uk: "uk-UA", af: "af-ZA"
        };
        return map[(lang || "en").split("-")[0]] || "en-US";
    }

    _toWav(pcm, rate) {
        const h = Buffer.alloc(44);
        h.write("RIFF", 0);
        h.writeUInt32LE(36 + pcm.length, 4);
        h.write("WAVE", 8);
        h.write("fmt ", 12);
        h.writeUInt32LE(16, 16);
        h.writeUInt16LE(1, 20);
        h.writeUInt16LE(1, 22);
        h.writeUInt32LE(rate, 24);
        h.writeUInt32LE(rate * 2, 28);
        h.writeUInt16LE(2, 32);
        h.writeUInt16LE(16, 34);
        h.write("data", 36);
        h.writeUInt32LE(pcm.length, 40);
        return Buffer.concat([h, pcm]);
    }

    _sendToUI(data) {
        try {
            if (this.ws?.readyState === 1) {
                this.ws.send(JSON.stringify(data));
            }
        } catch (e) { }
    }

    _sendChatHistory() {
        const session = this.activeSessions.get(this.roomId);
        if (!session) return;
        const history = (session.chatMessages || []).slice(-50);
        this._sendToUI({ event: "chat:history", messages: history });
    }

    _notifyPartner(event, data) {
        const session = this.activeSessions.get(this.roomId);
        if (!session) return;
        const partner = this.userType === "caller" ? session.receiverConnection : session.callerConnection;
        if (partner?.ws?.readyState === 1) {
            partner.ws.send(JSON.stringify({ event, ...data }));
        }
    }

    _getPartner() {
        const session = this.activeSessions.get(this.roomId);
        if (!session) return null;
        return this.userType === "caller" ? session.receiverConnection : session.callerConnection;
    }

    _isSameLanguage(partner) {
        const a = (this.myLanguage || "en").split("-")[0];
        const b = (partner.myLanguage || "en").split("-")[0];
        return a === b;
    }

    async cleanup() {
        if (this.sentenceTimer) {
            clearTimeout(this.sentenceTimer);
            this.sentenceTimer = null;
        }

        // Clear idle timer
        if (this._idleTimer) {
            clearTimeout(this._idleTimer);
            this._idleTimer = null;
        }

        // Process any remaining sentence
        if (this.sentence && this.sentence !== this.lastSentence) {
            this._finalizeSentence();
        }

        await this._stopStream();

        const session = this.activeSessions.get(this.roomId);
        if (session) {
            if (session.callerConnection === this) session.callerConnection = null;
            if (session.receiverConnection === this) session.receiverConnection = null;
        }

        this._notifyPartner("user_left", {});
        console.log(`🧹 Cleanup: ${this.userType} in ${this.roomId}`);
    }
}

module.exports = VoiceProcessor;