// server.js - Express + WebSocket Server for Live Translation
if (process.env.NODE_ENV !== "production") {
    require("dotenv").config();
    console.log("PORT =", process.env.PORT);
    console.log("NODE_ENV =", process.env.NODE_ENV);
}
// =====================================================
// PRIME TALKER - Supabase connection for email existence check
// =====================================================
const { createClient } = require("@supabase/supabase-js");

const supabaseAdmin = createClient(
    process.env.VITE_SUPABASE_URL,
    process.env.SUPABASE_SERVICE_ROLE_KEY
);

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
app.use((req, res, next) => {
    console.log("========== REQUEST ==========");
    console.log(req.method, req.originalUrl);
    console.log("=============================");
    next();
});
app.use(express.json());

// CORS - Allow cross-origin requests
app.use((req, res, next) => {
    console.log("🌍 CORS Middleware:", req.method, req.url);
    res.header("Access-Control-Allow-Origin", "*");
    res.header("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS");
    res.header("Access-Control-Allow-Headers", "*");
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
// PRIME TALKER - Check whether signup email already exists
// =====================================================
app.post("/api/check-email", async (req, res) => {
    try {
        const { email } = req.body;

        if (!email) {
            return res.status(400).json({
                exists: false,
                error: "Email is required"
            });
        }

        const normalizedEmail = email.trim().toLowerCase();

        // PRIME TALKER - Check existing users using Supabase Admin API
        const { data, error } = await supabaseAdmin.auth.admin.listUsers({
            page: 1,
            perPage: 1000,
        });

        if (error) {
            console.error("❌ Email check error:", error);
            return res.status(500).json({
                exists: false,
                error: "Unable to check email"
            });
        }
        // PRIME TALKER - Check whether the email belongs to an existing verified or unverified user
        const matchedUser = data.users.find(
            (user) => user.email?.toLowerCase() === normalizedEmail
        );
        const userExists = !!matchedUser;
        const emailVerified = matchedUser?.email_confirmed_at != null;
        return res.json({
            exists: userExists,
            verified: emailVerified
        });

    } catch (error) {
        console.error("❌ Check email exception:", error);

        return res.status(500).json({
            exists: false,
            error: "Unable to check email"
        });
    }
});


// =====================================================
// CONTACT US FORM SUBMISSION
// Direct email delivery using Brevo Transactional Email API
// Target recipient: primecontact@primetalker.com
// =====================================================
const fs = require("fs");

app.post("/api/contact", async (req, res) => {
    try {
        const { firstName, lastName, email, phone, subject, message } = req.body;

        // Identify the logged-in user using the Supabase access token
        let authenticatedUserId = null;

        const authHeader = req.headers.authorization;

        if (authHeader) {
            if (!authHeader.startsWith("Bearer ")) {
                return res.status(401).json({
                    success: false,
                    error: "Invalid authentication token."
                });
            }

            const accessToken = authHeader.split(" ")[1];

            const {
                data: { user },
                error: authError
            } = await supabaseAdmin.auth.getUser(accessToken);

            if (authError || !user) {
                return res.status(401).json({
                    success: false,
                    error: "Unable to verify user authentication."
                });
            }

            authenticatedUserId = user.id;
        }


        // Validate required fields
        if (
            !firstName?.trim() ||
            !email?.trim() ||
            !message?.trim()
        ) {
            return res.status(400).json({
                success: false,
                error: "Missing required fields (firstName, email, message)"
            });
        }

        const recipientEmail = "primecontact@primetalker.com";
        const timestamp = new Date().toISOString();

        // Save contact form submission in Supabase
        const { data: savedSubmission, error: databaseError } =
            await supabaseAdmin
                .from("contact_submissions")
                .insert({
                    user_id: authenticatedUserId,
                    first_name: firstName.trim(),
                    last_name: lastName?.trim() || null,
                    email: email.trim(),
                    phone: phone?.trim() || null,
                    subject: subject?.trim() || null,
                    message: message.trim(),
                    email_status: "pending"
                })
                .select("id")
                .single();

        if (databaseError) {
            console.error(
                "❌ Failed to save contact submission in Supabase:",
                databaseError
            );

            return res.status(500).json({
                success: false,
                error: "Unable to save your message. Please try again."
            });
        }

        console.log(
            "✅ Contact submission saved in Supabase:",
            savedSubmission.id
        );


        const submissionData = {
            id: savedSubmission.id,
            timestamp,
            recipientEmail,
            sender: {
                name: `${firstName} ${lastName || ""}`.trim(),
                email: email.trim(),
                phone: phone ? phone.trim() : "N/A"
            },
            subject: subject?.trim() || "PrimeTalker Contact Us Enquiry",
            message: message.trim()
        };

        // Log contact form submission
        console.log("\n====================================================");
        console.log("📩 NEW CONTACT FORM SUBMISSION RECEIVED");
        console.log(`Target Recipient : ${recipientEmail}`);
        console.log(`From             : ${submissionData.sender.name} <${submissionData.sender.email}>`);
        console.log(`Phone            : ${submissionData.sender.phone}`);
        console.log(`Subject          : ${submissionData.subject}`);
        console.log(`Timestamp        : ${timestamp}`);
        console.log("Message Content  :");
        console.log(submissionData.message);
        console.log("====================================================\n");

        // Preserve existing local JSON logging
        try {
            const logFilePath = path.join(__dirname, "contact_submissions.json");
            let submissions = [];

            if (fs.existsSync(logFilePath)) {
                try {
                    submissions = JSON.parse(
                        fs.readFileSync(logFilePath, "utf8")
                    );
                } catch (err) {
                    submissions = [];
                }
            }

            submissions.push(submissionData);

            fs.writeFileSync(
                logFilePath,
                JSON.stringify(submissions, null, 2),
                "utf8"
            );
        } catch (logError) {
            console.error("Contact submission logging failed:", logError);
        }

        // Get Brevo API key from backend environment variables
        const brevoApiKey = process.env.BREVO_API_KEY;

        if (!brevoApiKey) {
            console.error("BREVO_API_KEY is not configured.");

            return res.status(500).json({
                success: false,
                error: "Email service is not configured."
            });
        }

        // Escape user-provided content before inserting into HTML
        const escapeHtml = (value) =>
            String(value)
                .replace(/&/g, "&amp;")
                .replace(/</g, "&lt;")
                .replace(/>/g, "&gt;")
                .replace(/"/g, "&quot;")
                .replace(/'/g, "&#039;");

        // Prepare plain-text email
        const emailBody = `
Hello Prime Talker Team,

A new message has been submitted through the Prime Talker Contact Us form.

Name: ${submissionData.sender.name}
Email Address: ${submissionData.sender.email}
Phone Number: ${submissionData.sender.phone}

Subject: ${submissionData.subject}

Message:
${submissionData.message}

Submission ID: ${submissionData.id}
Submitted At: ${timestamp}

Thank you,
Prime Talker Website
        `.trim();

        // Prepare HTML email
        const htmlContent = `
            <div style="font-family: Arial, sans-serif; color: #333; line-height: 1.6;">
                <h2 style="color: #333;">New Contact Us Message</h2>

                <p>A new message has been submitted through the Prime Talker website.</p>

                <table style="border-collapse: collapse; width: 100%;">
                    <tr>
                        <td style="padding: 8px; font-weight: bold;">Name</td>
                        <td style="padding: 8px;">${escapeHtml(submissionData.sender.name)}</td>
                    </tr>
                    <tr>
                        <td style="padding: 8px; font-weight: bold;">Email</td>
                        <td style="padding: 8px;">${escapeHtml(submissionData.sender.email)}</td>
                    </tr>
                    <tr>
                        <td style="padding: 8px; font-weight: bold;">Phone</td>
                        <td style="padding: 8px;">${escapeHtml(submissionData.sender.phone)}</td>
                    </tr>
                    <tr>
                        <td style="padding: 8px; font-weight: bold;">Subject</td>
                        <td style="padding: 8px;">${escapeHtml(submissionData.subject)}</td>
                    </tr>
                </table>

                <h3>Message</h3>
                <div style="padding: 12px; background: #f5f5f5; border-radius: 6px; white-space: pre-wrap;">${escapeHtml(submissionData.message)}</div>

                <p style="font-size: 12px; color: #777;">
                    Submission ID: ${submissionData.id}<br/>
                    Submitted At: ${timestamp}
                </p>
            </div>
        `;

        // Send email directly using Brevo API
        const brevoResponse = await fetch(
            "https://api.brevo.com/v3/smtp/email",
            {
                method: "POST",
                headers: {
                    "accept": "application/json",
                    "api-key": brevoApiKey,
                    "content-type": "application/json"
                },
                body: JSON.stringify({
                    sender: {
                        name: "Prime Talker",
                        email: recipientEmail
                    },
                    to: [
                        {
                            email: recipientEmail,
                            name: "Prime Talker Team"
                        }
                    ],
                    replyTo: {
                        email: submissionData.sender.email,
                        name: submissionData.sender.name
                    },
                    subject: submissionData.subject,
                    textContent: emailBody,
                    htmlContent: htmlContent
                })
            }
        );

        const brevoResult = await brevoResponse.json();

        // Check Brevo API response

        if (!brevoResponse.ok) {
            console.error("Brevo email sending failed:", brevoResult);

            // Update Supabase record when email sending fails
            const { error: updateError } = await supabaseAdmin
                .from("contact_submissions")
                .update({ email_status: "failed" })
                .eq("id", savedSubmission.id);

            if (updateError) {
                console.error(
                    "❌ Failed to update contact email status:",
                    updateError
                );
            }

            return res.status(502).json({
                success: false,
                error: "Unable to send your message right now. Please try again later."
            });
        }

        console.log("✅ Contact email accepted by Brevo.");
        console.log("Brevo Message ID:", brevoResult.messageId);

        // Update Supabase record after successful email delivery
        const { error: updateError } = await supabaseAdmin
            .from("contact_submissions")
            .update({ email_status: "sent" })
            .eq("id", savedSubmission.id);

        if (updateError) {
            console.error(
                "❌ Failed to update contact email status:",
                updateError
            );
        }


        return res.status(200).json({
            success: true,
            message: "Your message has been sent successfully!",
            submissionId: submissionData.id
        });

    } catch (error) {
        console.error("❌ Error handling contact submission:", error);

        return res.status(500).json({
            success: false,
            error: "Failed to process your message."
        });
    }
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
        //store creator's selected voice so tts can use the speaker's voice
        const { creatorLanguage, creatorName, creatorVoice } = req.body;
        const roomId = uuidv4().substring(0, 8);

        activeSessions.set(roomId, {
            creatorLanguage,
            creatorName,
            creatorVoice: creatorVoice || "male",
            participantLanguage: null,
            participantName: null,
            participantActive: false,
            callerConnection: null,
            receiverConnection: null,
            chatMessages: [],
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
        // NEW: Allow only one active guest participant at a time
        if (session.participantActive) {
            return res.status(409).json({
                error: "ROOM_FULL",
                message: "This meeting already has two participants."
            });
        }

        // Allow re-joining - update participant info
        session.participantLanguage = participantLanguage;
        session.participantVoice = participantVoice;
        session.participantName = participantName;
        session.participantActive = true;//new: mark participant as active
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

    console.log("========== REQUEST ==========");
    console.log(req.method, req.originalUrl);
    console.log("=============================");

    const roomId = req.query.roomId;

    console.log("Requested Room:", roomId);
    console.log("Current Rooms:", [...activeSessions.keys()]);

    const session = activeSessions.get(roomId);

    if (!session) {
        console.log("ROOM NOT FOUND");
        return res.status(404).json({
            error: "Room not found"
        });
    }

    console.log("ROOM FOUND");

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
                // FEATURE: Notify the guest before closing connections and deleting the room.
                const guestWs = session.receiverConnection?.ws;

                if (guestWs?.readyState === 1) {
                    guestWs.send(JSON.stringify({
                        event: "meeting_ended",
                        creatorName: session.creatorName || "Creator",
                        creatorLanguage: session.creatorLanguage || "Unknown"
                    }));
                }

                // Creator leaving - close all connections and delete room.
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
                session.participantActive = false;//new: mark participant as inactive
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

    ws.on("close", (code, reason) => {
        console.log("==================================");
        console.log("❌ WebSocket closed");
        console.log("Close Code:", code);
        console.log("Reason:", reason.toString());
        console.log("==================================");

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

    console.log("================================");
    console.log("UPGRADE REQUEST");
    console.log("URL:", req.url);
    console.log("================================");

    if (req.url.startsWith("/audio-stream")) {

        console.log("✅ Upgrading WebSocket");

        wss.handleUpgrade(req, socket, head, (ws) => {
            wss.emit("connection", ws, req);
        });

    } else {

        console.log("❌ Unknown Upgrade:", req.url);

        socket.destroy();
    }
});