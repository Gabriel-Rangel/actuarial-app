// static/js/scripts.js

document.getElementById("genie-form").addEventListener("submit", async function (e) {
    e.preventDefault();

    const queryInput = document.getElementById("user-query");
    const query = queryInput.value.trim(); // Get user input
    const conversationHistory = document.getElementById("conversation-history");
    const loadingBar = document.getElementById("loading-bar");
    const urlParams = new URLSearchParams(window.location.search);
    const app = urlParams.get('app');  // Get app from URL

    if (!query) return; // Prevent empty submissions

    // Add user message to conversation history
    const userMessage = document.createElement("div");
    userMessage.className = "chat-bubble user";
    userMessage.textContent = query;
    conversationHistory.appendChild(userMessage);

    // Clear input field and show loading bar
    queryInput.value = "";
    loadingBar.style.display = "block"; // Show loading bar

    try {
        // Send request to Flask backend (Genie endpoint)
        console.log('Sending request with:', { question: query, app: app });
        const response = await fetch("/api/genie/start_conversation", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ question: query, app: app }),
        });

        if (!response.ok) {
            console.error('Response not OK:', response.status, response.statusText);
            throw new Error(`HTTP error! status: ${response.status}`);
        }

        console.log('Response status:', response.status);
        const data = await response.json();
        console.log('Response data:', data);

        // Add AI response to conversation history
        const botMessage = document.createElement("div");
        botMessage.className = "chat-bubble bot";

        if (data.error) {
            console.error('Error in response:', data.error);
            botMessage.textContent = data.error;
        } else {
            botMessage.textContent = data;
        }

        conversationHistory.appendChild(botMessage);
    } catch (error) {
        console.error("Error communicating with Genie:", error);
        console.error("Error details:", error.message);

        // Add error message to conversation history
        const errorMessage = document.createElement("div");
        errorMessage.className = "chat-bubble bot";
        errorMessage.textContent = "Error: " + error.message;
        conversationHistory.appendChild(errorMessage);
    } finally {
        // Hide loading bar and scroll to the bottom of the chat section
        loadingBar.style.display = "none"; // Hide loading bar
        conversationHistory.scrollTop = conversationHistory.scrollHeight;
    }
});