const io = require('socket.io-client');
const fs = require('fs');
const path = require('path');

const socket = io('http://localhost:8000');
const inputFilePath = 'input.txt';
const outputFilePath = path.join(__dirname, `output_${process.pid}.json`);

const prompts = fs.readFileSync(inputFilePath, 'utf-8').split('\n');

prompts.forEach((prompt, index) => {
    socket.emit('sendPrompt', { prompt: prompt.trim(), clientId: socket.id });
});

socket.on('response', (data) => {
    const { ClientID, Prompt, Message, TimeSent, TimeRecvd, Source } = data;

    const promptResponse = {
        "ClientID": ClientID,
        "Prompt": Prompt,
        "Message": Message,
        "TimeSent": TimeSent,
        "TimeRecvd": TimeRecvd,
        "Source": Source
    };

    let outputData = [];
    if (fs.existsSync(outputFilePath)) {
        outputData = JSON.parse(fs.readFileSync(outputFilePath, 'utf-8'));
    }

    outputData.push(promptResponse);
    fs.writeFileSync(outputFilePath, JSON.stringify(outputData, null, 4), 'utf-8');
    console.log(`Response saved to ${outputFilePath}`);
});

console.log('Client connected');

