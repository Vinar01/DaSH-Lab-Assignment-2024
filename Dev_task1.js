const fs = require('fs');
const axios = require('axios');

const url = 'https://api-inference.huggingface.co/models/google/gemma-2b';
const apiKey = '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';  

// Function to make the API call
async function apiCall(prompt) {
    try {
        const response = await axios.post(
            url,
            { inputs: prompt },
            {
                headers: {
                    Authorization: 'Bearer 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',
                    'Content-Type': 'application/json'
                }
            }
        );
        return response.data;  // Modify this based on how the response is structured
    } catch (error) {
        console.error('Error making API call:', error);
        return "Error";  // Return an error message or handle it as needed
    }
}

// Read prompts from input.txt
const prompts = fs.readFileSync('input.txt', 'utf-8').split('\n').filter(Boolean);

// Array to store the output JSON objects
const outputData = [];

async function processPrompts() {
    for (const prompt of prompts) {
        // Record the time when the prompt was sent
        const timeSent = Math.floor(Date.now() / 1000);

        // Send the prompt to an API and get the response
        const responseMessage = await apiCall(prompt);

        // Record the time when the response was received
        const timeRecvd = Math.floor(Date.now() / 1000);

        // Create the JSON object for this prompt-response pair
        const promptResponse = {
            "Prompt": prompt,
            "Message": responseMessage[0],
            "TimeSent": timeSent,
            "TimeRecvd": timeRecvd,
            "Source": "Gemma"  // Replace with actual source if different
        };

        // Add the JSON object to the array
        outputData.push(promptResponse);
    }

    // Write the output data to output.json
    fs.writeFileSync('output.json', JSON.stringify(outputData, null, 4), 'utf-8');
    console.log("Output saved to output.json");
}

// Start processing prompts
processPrompts();