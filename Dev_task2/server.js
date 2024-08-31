const fs = require('fs');
const http = require('http');
const socket = require('socket.io');
const path = require('path');
const axios = require('axios');

const server = http.createServer();
const io = socket(server);

const apiKey = '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';
const url = 'https://api-inference.huggingface.co/models/google/gemma-2b';

// Function to make the API call
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
        return response.data[0].generated_text; // Adjust based on actual response format
    } catch (error) {
        console.error('Error while making API call', error);
        return 'Error';
    }
}

const prompts = fs.readFileSync('input.txt', 'utf-8').split('\n');

io.on('connection', (socket) => {
    console.log(`Client connected: ${socket.id}`);

    // Distribute prompts to each client
    let clientIndex = 0;
    for (let i = 0; i < prompts.length; i++) {
        const clientId = socket.id;

        // Make API call on the server
        apiCall(prompts[i]).then((responseMessage) => {
            const timeSent = Math.floor(Date.now() / 1000);
            const timeRecvd = timeSent; // Simulating same timestamp for simplicity

            const promptResponse = {
                "ClientID": clientId,
                "Prompt": prompts[i],
                "Message": responseMessage,
                "TimeSent": timeSent,
                "TimeRecvd": timeRecvd,
                "Source": "Gemma"
            };

            const outputFilePath = path.join(__dirname, `output_${clientId}.json`);
            let outputData = [];

            if (fs.existsSync(outputFilePath)) {
                outputData = JSON.parse(fs.readFileSync(outputFilePath, 'utf-8'));
            }

            outputData.push(promptResponse);
            fs.writeFileSync(outputFilePath, JSON.stringify(outputData, null, 4), 'utf-8');
            console.log(`Response saved to output_${clientId}.json`);

            // Send the response to the client
            io.to(clientId).emit('response', promptResponse);
        });

        clientIndex++;
        if (clientIndex >= io.sockets.sockets.size) {
            clientIndex = 0;
        }
    }
});

server.listen(3000, () => {
    console.log('Server is running on port 3000');
});
