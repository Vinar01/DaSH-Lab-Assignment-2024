const io = require('socket.io-client');
const fs = require('fs');
const path = require('path');

const socket = io('http://localhost:3000');

socket.on('response', (data) => {
    const { Prompt, Message, TimeSent, TimeRecvd, ClientID, Source } = data;

    const promptResponse = {
        "ClientID": ClientID,
        "Prompt": Prompt,
        "Message": Message,
        "TimeSent": TimeSent,
        "TimeRecvd": TimeRecvd,
        "Source": Source
    };

    const outputFilePath = path.join(__dirname, `output_${ClientID}.json`);
    let outputData = [];

    if (fs.existsSync(outputFilePath)) {
        outputData = JSON.parse(fs.readFileSync(outputFilePath, 'utf-8'));
    }

    outputData.push(promptResponse);
    fs.writeFileSync(outputFilePath, JSON.stringify(outputData, null, 4), 'utf-8');
    console.log(`Response saved to output_${ClientID}.json`);
});

console.log('Client connected');
