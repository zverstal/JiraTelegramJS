require('dotenv').config();
const fs = require('fs');
const localtunnel = require('localtunnel');
const axios = require('axios');
const { exec } = require('child_process');

async function startTunnel() {
    const tunnel = await localtunnel({ port: 3000 });
    console.log('Tunnel URL:', tunnel.url);

    const envPath = '.env';
    let envContent = fs.readFileSync(envPath, 'utf8');

    if (envContent.includes('PUBLIC_BASE_URL=')) {
        envContent = envContent.replace(/PUBLIC_BASE_URL=.*/g, `PUBLIC_BASE_URL=${tunnel.url}`);
    } else {
        envContent += `\nPUBLIC_BASE_URL=${tunnel.url}`;
    }

    fs.writeFileSync(envPath, envContent);

    // ❌ Уведомление в Telegram убрано

    // Перезапуск бота через pm2
    exec('pm2 restart jirabot', (error, stdout, stderr) => {
        if (error) {
            console.error(`Ошибка перезапуска бота: ${error.message}`);
            return;
        }
        console.log(`Бот перезапущен: ${stdout}`);
    });

    tunnel.on('close', () => {
        console.log('Tunnel closed');
        process.exit(1);
    });

    tunnel.on('error', (err) => {
        console.error('Tunnel error:', err);
        process.exit(1);
    });
}

startTunnel();


