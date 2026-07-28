// server.js - Express + WebSocket Server for Live Translation
if (process.env.NODE_ENV !== "production") {
    require("dotenv").config();
}

// Force immediate log output
if (process.stdout._handle) process.stdout._handle.setBlocking(true);
if (process.stderr._handle) process.stderr._handle.setBlocking(true);
const express = require("express");
const { v4: uuidv4 } = require("uuid");
const WebSocket = require("ws");
const path = require("path");

// Twilio Video Token Generation
const { AccessToken } = require('twilio').jwt;
const { VideoGrant } = AccessToken;

const app = express();
app.use(express.json());

// CORS - Allow cross-origin requests
app.use((req, res, next) => {
    res.header("Access-Control-Allow-Origin", "*");
    res.header("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS");
    res.header("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept");
    if (req.method === "OPTIONS") {
        return res.sendStatus(200);
    }
    next();
});

app.use(express.static(path.join(__dirname, "public")));

// Active translation sessions
const activeSessions = new Map();

// Health check
app.get("/health", (req, res) => {
    res.json({ status: "ok", activeRooms: activeSessions.size });
});

// =====================================================
// TWILIO VIDEO TOKEN
// =====================================================
app.post("/api/video-token", (req, res) => {
    try {
        const { identity, roomName } = req.body;

        if (!identity || !roomName) {
            return res.status(400).json({ error: "Missing identity or roomName" });
        }

        const twilioAccountSid = process.env.TWILIO_ACCOUNT_SID;
        const twilioApiKeySid = process.env.TWILIO_API_KEY_SID;
        const twilioApiSecret = process.env.TWILIO_API_SECRET;

        if (!twilioAccountSid || !twilioApiKeySid || !twilioApiSecret) {
            return res.status(501).json({ error: "Twilio not configured - video disabled" });
        }

        const token = new AccessToken(
            twilioAccountSid,
            twilioApiKeySid,
            twilioApiSecret,
            { identity }
        );

        const videoGrant = new VideoGrant({ room: roomName });
        token.addGrant(videoGrant);

        console.log("✅ Video token generated for:", identity, "in room:", roomName);
        res.json({ token: token.toJwt() });
    } catch (error) {
        console.error("Video token error:", error);
        res.status(500).json({ error: "Failed to generate video token" });
    }
});

// Create a new room
app.post("/create-room", (req, res) => {
    try {
        const { creatorLanguage, creatorName } = req.body;
        const roomId = uuidv4().substring(0, 8);

        activeSessions.set(roomId, {
            creatorLanguage,
            creatorName,
            participantLanguage: null,
            participantName: null,
            callerConnection: null,
            receiverConnection: null,
            createdAt: Date.now()
        });

        const joinUrl = `${req.protocol}://${req.get("host")}/join.html?room=${roomId}`;
        console.log("✅ Room created:", roomId);

        res.json({ roomId, joinUrl });
    } catch (error) {
        console.error("Create room error:", error);
        res.status(500).json({ error: "Failed to create room" });
    }
});

// Join an existing room (allows re-joining)
app.post("/join-room", (req, res) => {
    try {
        const { roomId, participantLanguage, participantVoice, participantName } = req.body;
        const session = activeSessions.get(roomId);

        if (!session) {
            return res.status(404).json({ error: "Room not found" });
        }

        // Allow re-joining - update participant info
        session.participantLanguage = participantLanguage;
        session.participantVoice = participantVoice;
        session.participantName = participantName;
        activeSessions.set(roomId, session);

        console.log("✅ User joined room:", roomId);

        res.json({
            success: true,
            creatorLanguage: session.creatorLanguage,
            creatorName: session.creatorName
        });
    } catch (error) {
        console.error("Join room error:", error);
        res.status(500).json({ error: "Failed to join room" });
    }
});

// Get room info
app.get("/room-info", (req, res) => {
    const roomId = req.query.roomId;
    const session = activeSessions.get(roomId);

    if (!session) {
        return res.status(404).json({ error: "Room not found" });
    }

    res.json({
        creatorLanguage: session.creatorLanguage,
        creatorName: session.creatorName,
        participantLanguage: session.participantLanguage,
        participantName: session.participantName
    });
});

// Leave room (allows re-joining - only clears user's connection)
app.post("/leave-room", (req, res) => {
    try {
        const { roomId, userType } = req.body;
        const session = activeSessions.get(roomId);

        if (session) {
            if (userType === "caller") {
                // Creator leaving - close all connections and delete room
                if (session.callerConnection?.ws) {
                    session.callerConnection.ws.close();
                }
                if (session.receiverConnection?.ws) {
                    session.receiverConnection.ws.close();
                }
                activeSessions.delete(roomId);
                console.log("🗑️ Room deleted (creator left):", roomId);
            } else {
                // Participant leaving - just clear their connection, keep room alive
                if (session.receiverConnection?.ws) {
                    session.receiverConnection.ws.close();
                }
                session.receiverConnection = null;
                session.participantLanguage = null;
                session.participantName = null;
                console.log("👋 Participant left room (room still active):", roomId);
            }
        }

        res.json({ success: true });
    } catch (error) {
        console.error("Leave room error:", error);
        res.status(500).json({ error: "Failed to leave room" });
    }
});

// WebSocket server
const VoiceProcessor = require("./voice-processor");
const wss = new WebSocket.Server({ noServer: true });

wss.on("connection", (ws, req) => {
    console.log("🔗 WebSocket connected from", req.socket.remoteAddress);

    const processor = new VoiceProcessor(ws, activeSessions);

    ws.on("message", async (message) => {
        try {
            const data = JSON.parse(message);
            await processor.handleMessage(data);
        } catch (error) {
            console.error("Message error:", error.message);
        }
    });

    ws.on("close", () => {
        console.log("❌ WebSocket closed");
        processor.cleanup();
    });

    ws.on("error", (err) => {
        console.error("WebSocket error:", err.message);
        processor.cleanup();
    });
});

// Start server
const PORT = process.env.PORT || 8080;
const server = app.listen(PORT, () => {
    console.log(`🚀 Server running on port ${PORT}`);
});

// Handle WebSocket upgrade for /audio-stream path
server.on("upgrade", (req, socket, head) => {
    if (req.url.startsWith("/audio-stream")) {
        wss.handleUpgrade(req, socket, head, (ws) => {
            wss.emit("connection", ws, req);
        });
    } else {
        socket.destroy();
    }
});