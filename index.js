require('dotenv').config();
const { Bot, InlineKeyboard } = require('grammy');
const axios = require('axios');
const sqlite3 = require('sqlite3').verbose();

const bot = new Bot(process.env.BOT_API_KEY);
const db = new sqlite3.Database('tasks.db');

db.serialize(() => {
    db.run(`CREATE TABLE IF NOT EXISTS tasks (
        id TEXT PRIMARY KEY,
        title TEXT,
        priority TEXT,
        department TEXT,
        lastSent DATE
    )`);

    db.run(`CREATE TABLE IF NOT EXISTS user_actions (
        username TEXT,
        taskId TEXT,
        action TEXT,
        timestamp DATE,
        FOREIGN KEY(taskId) REFERENCES tasks(id)
    )`);
});

function getPriorityEmoji(priority) {
    const emojis = {
        Blocker: '🚨',
        High: '🔴',
        Medium: '🟡',
        Low: '🟢'
    };
    return emojis[priority] || '';
}

async function fetchJiraTaskStatus(taskId) {
    try {
        const response = await axios.get(`https://jira.sxl.team/rest/api/2/issue/${taskId}`, {
            headers: {
                'Authorization': `Bearer ${process.env.JIRA_PAT}`,
                'Accept': 'application/json'
            }
        });
        return response.data.fields.status.name;
    } catch (error) {
        console.error('Error fetching Jira task status:', error);
        return null;
    }
}

async function sendJiraTasks(ctx) {
    const isWeekend = [0, 6].includes(new Date().getDay());
    const query = `SELECT * FROM tasks WHERE ${isWeekend ? 'department = "Техническая поддержка"' : '1=1'} AND lastSent < datetime("now", "-3 days")`;

    db.all(query, [], async (err, rows) => {
        if (err) {
            throw err;
        }
        for (const task of rows) {
            const currentStatus = await fetchJiraTaskStatus(task.id);
            if (!currentStatus || currentStatus !== 'Open') {
                db.run('DELETE FROM tasks WHERE id = ?', [task.id]);
                continue;
            }

            const keyboard = new InlineKeyboard()
                .text('Взять в работу', `take_task:${task.id}`)
                .text('В курсе', `aware_task:${task.id}`);

            const messageText = `Задача: ${task.id}\nОписание: ${task.title}\nПриоритет: ${getPriorityEmoji(task.priority)}\nОтдел: ${task.department}`;
            await ctx.reply(messageText, { reply_markup: keyboard });

            db.run('UPDATE tasks SET lastSent = ? WHERE id = ?', [new Date(), task.id]);
        }
    });
}

bot.command('start', async (ctx) => {
    await ctx.reply('Привет! Я буду присылать новые задачи каждый день.');
    sendJiraTasks(ctx);
    setInterval(() => sendJiraTasks(ctx), 86400000); // 24 часа
});

const usernameMappings = {
    "lipchinski": "Дмитрий Селиванов",
    "YurkovOfficial": "Пётр Юрков",
    "Jlufi": "Даниил Маслов",
    "EuroKaufman": "Даниил Баратов"
};

bot.callbackQuery(/^take_task:(.+)$/, async (ctx) => {
    const taskId = ctx.match[1];
    const success = await updateJiraTaskStatus(taskId);
    if (success) {
        const username = usernameMappings[ctx.from.username] || ctx.from.username;
        await ctx.editMessageText(`Задача ${taskId} взята в работу пользователем ${username}.`);
        db.run('INSERT INTO user_actions (username, taskId, action, timestamp) VALUES (?, ?, ?, ?)', [ctx.from.username, taskId, 'take_task', new Date()]);
    } else {
        await ctx.reply(`Не удалось взять задачу ${taskId} в работу. Попробуйте снова.`);
    }
});


bot.callbackQuery(/^aware_task:(.+)$/, async (ctx) => {
    const taskId = ctx.match[1];
    db.run('INSERT INTO user_actions (username, taskId, action, timestamp) VALUES (?, ?, ?, ?)', [ctx.from.username, taskId, 'aware_task', new Date()]);

    // Получение списка пользователей, отметивших задачу как 'в курсе'
    db.all('SELECT DISTINCT username FROM user_actions WHERE taskId = ? AND action = "aware_task"', [taskId], (err, rows) => {
        if (err) {
            console.error('Error fetching aware users:', err);
            return;
        }
        const awareUsers = rows.map(row => usernameMappings[row.username] || row.username).join(', ');
        const updatedMessage = `Специалисты в курсе задачи: ${awareUsers}`;
        
        // Обновление сообщения
        ctx.editMessageText(updatedMessage);
    });

    db.run('UPDATE tasks SET lastSent = datetime("now", "+3 days") WHERE id = ?', [taskId]);
});


async function updateJiraTaskStatus(taskId) {
    try {
        const transitionId = '221'; // Замените на актуальный ID
        const transitionResponse = await axios.post(`https://jira.sxl.team/rest/api/2/issue/${taskId}/transitions`, {
            transition: {
                id: transitionId
            }
        }, {
            headers: {
                'Authorization': `Bearer ${process.env.JIRA_PAT}`,
                'Content-Type': 'application/json'
            }
        });
        return transitionResponse.status === 204;
    } catch (error) {
        console.error('Error updating Jira task:', error);
        return false;
    }
}

bot.start();
