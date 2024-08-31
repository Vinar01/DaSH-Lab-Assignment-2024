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
        return response.data;  
    } catch (error) {
        console.error('Error making API call:', error);
        return "Error"; 
    }
}


const prompts = fs.readFileSync('input.txt', 'utf-8').split('\n').filter(Boolean);


const outputData = [];

async function processPrompts() {
    for (const prompt of prompts) {
        
        const timeSent = Math.floor(Date.now() / 1000);
        const responseMessage = await apiCall(prompt);     
        const timeRecvd = Math.floor(Date.now() / 1000);
        
        const promptResponse = {
            "Prompt": prompt,
            "Message": responseMessage[0],
            "TimeSent": timeSent,
            "TimeRecvd": timeRecvd,
            "Source": "Gemma"  
        };

        
        outputData.push(promptResponse);
    }

   
    fs.writeFileSync('output.json', JSON.stringify(outputData, null, 4), 'utf-8');
    console.log("Output saved to output.json");
}


processPrompts();
