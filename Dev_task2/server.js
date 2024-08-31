const fs = require('fs');
const http = require('http');
const socket = require('socket.io');
const axios = require('axios');

const server = http.createServer();
const io = socket(server);

const apiKey = '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';  
const url = 'https://api-inference.huggingface.co/models/google/gemma-2b';


async function apiCall(prompt) {
    try {
        const response = await axios.post(
            url,
            { inputs: prompt },
            {
                headers: {
                    Authorization: `Bearer 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`,
                    'Content-Type': 'application/json',
                },
            }
        );
        return response.data[0].generated_text;  
    } catch (error) {
        console.error('Error while making API call:', error);
        return 'Error';
    }
}

io.on('connection', (socket) => {
    console.log(`Client connected: ${socket.id}`);

    socket.on('sendPrompt', async (data) => {
        const { prompt, clientId } = data;

        
        const responseMessage = await apiCall(prompt);
        const timeSent = Math.floor(Date.now() / 1000);
        const timeRecvd = timeSent; 
        
        const promptResponse = {
            "ClientID": clientId,
            "Prompt": prompt,
            "Message": responseMessage,
            "TimeSent": timeSent,
            "TimeRecvd": timeRecvd,
            "Source": "Gemma"
        };

        
        io.emit('response', promptResponse);
    });

    socket.on('disconnect', () => {
        console.log(`Client disconnected: ${socket.id}`);
    });
});

server.listen(8000, () => {
    console.log('Server is running on port 8000');
});
