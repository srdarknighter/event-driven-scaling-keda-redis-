import http from 'k6/http';
import { sleep } from 'k6';

export const options = {
    stages: [
        { duration: '20s', target: 35 },
        { duration: '40s', target: 100 }, 
        { duration: '20s', target: 0 }, 
    ],
};

function randomString(length) {
    let chars = 'abcdefghijklmnopqrstuvwxyz0123456789';
    let result = '';
    for (let i = 0; i < length; i++) {
        result += chars.charAt(Math.floor(Math.random() * chars.length));
    }
    return result;
}

export default function () {
    let tier = Math.random() > (1 / 6) ? "free" : "premium";
    const payload = JSON.stringify({
        submission_id: randomString(8),
        user_id: 'user_' + randomString(5),
        language: 'python',
        tier: tier
    });

    const params = {
        headers: { 'Content-Type': 'application/json' }
    };

    http.post('http://localhost:8080/submit', payload, params);

    sleep(0.1);
}