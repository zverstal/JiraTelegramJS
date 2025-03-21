require('dotenv').config();
const fs = require('fs');
const localtunnel = require('localtunnel');
const axios = require('axios');

async function startTunnel() {
    const tunnel = await localtunnel({ port: 3000 });
    console.log('Tunnel URL:', tunnel.url);

    // Сохраняем URL в .env-файл
    const envPath = '.env';
    let envContent = fs.readFileSync(envPath, 'utf8');

    if (envContent.includes('PUBLIC_BASE_URL=')) {
        envContent = envContent.replace(/PUBLIC_BASE_URL=.*/g, `PUBLIC_BASE_URL=${tunnel.url}`);
    } else {
        envContent += `\nPUBLIC_BASE_URL=${tunnel.url}`;
    }

    fs.writeFileSync(envPath, envContent);

    // Уведомляем в Telegram
    await axios.post(`https://api.telegram.org/bot${process.env.BOT_API_KEY}/sendMessage`, {
        chat_id: process.env.ADMIN_CHAT_ID,
        text: `🔗 Туннель обновлён:\n${tunnel.url}`
    });

    tunnel.on('close', () => {
        console.log('Tunnel closed');
        process.exit(1); // pm2 автоматически перезапустит туннель
    });

    tunnel.on('error', (err) => {
        console.error('Tunnel error:', err);
        process.exit(1);
    });
}

startTunnel();
