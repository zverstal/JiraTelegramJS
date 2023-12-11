require('dotenv').config();
const { Bot, InlineKeyboard } = require('grammy');
const axios = require('axios');

const bot = new Bot(process.env.BOT_API_KEY);

// Функция для получения эмодзи по приоритету задачи
function getPriorityEmoji(priority) {
    const emojis = {
        Blocker: '🚨',
        High: '🔴',
        Medium: '🟡',
        Low: '🟢'
    };
    return emojis[priority] || '';
}

async function fetchJiraTasks() {
    try {
        const response = await axios.get('https://jira.sxl.team/rest/api/2/search', {
            headers: {
                'Authorization': `Bearer ${process.env.JIRA_PAT}`,
                'Accept': 'application/json'
            },
            params: {
                jql: 'project = SUPPORT AND (Отдел = "Техническая поддержка" or Отдел = "QA" or Отдел = "Sportsbook") and status = "Open"'
            }
        });

        return response.data.issues.map(issue => ({
            id: issue.key, // 'key' задачи
            title: issue.fields.summary,
            priority: issue.fields.priority.name,
        }));
    } catch (error) {
        console.error('Error fetching Jira tasks:', error);
        return [];
    }
}

async function getTaskDetails(taskId) {
    try {
        const response = await axios.get(`https://jira.sxl.team/rest/api/2/issue/${taskId}`, {
            headers: {
                'Authorization': `Bearer ${process.env.JIRA_PAT}`,
                'Accept': 'application/json'
            }
        });

        return response.data; // Возвращает полный объект задачи
    } catch (error) {
        console.error('Error fetching Jira task details:', error);
        return null;
    }
}



const sentTasks = new Map();

async function sendJiraTasks(ctx) {
    const tasks = await fetchJiraTasks();
    const now = new Date();

    for (const task of tasks) {
        const lastSentTime = sentTasks.get(task.id);
        const timeDiff = now - lastSentTime;

        if (!lastSentTime || timeDiff > 86400000) {
            const taskDetails = await getTaskDetails(task.id);

            if (taskDetails) {
                const department = taskDetails.fields.customfield_10500 ? taskDetails.fields.customfield_10500.value : 'Не указан';
                const keyboard = new InlineKeyboard();

                if (department === "Техническая поддержка") {
                    keyboard.text('Взять в работу', `take_task:${task.id}`);
                } else if (department === "QA") {
                    keyboard.text('В курсе', `aware_task:${task.id}`);
                }
                else if (department === "Sportsbook") {
                    keyboard.text('В курсе', `aware_task:${task.id}`);
                }

                const messageText = `Задача: ${task.id}\nСсылка: https://jira.sxl.team/browse/${task.id}\nОписание: ${task.title}\nПриоритет: ${getPriorityEmoji(task.priority)}\nОтдел: ${department}`;
                ctx.reply(messageText, { reply_markup: keyboard });
                sentTasks.set(task.id, now);
            }
        }
    }
}




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

const usernameMappings = {
    "lipchinski": "Дмитрий Селиванов",
    "YurkovOfficial": "Пётр Юрков",
    "Jlufi": "Даниил Маслов",
    "EuroKaufman": "Даниил Баратов"
};



bot.callbackQuery(/^take_task:(.+)$/, async (ctx) => {
    const taskId = ctx.match[1];
    const task = await getTaskDetails(taskId);

    if (task && task.fields.customfield_10500 && task.fields.customfield_10500.value === "Техническая поддержка") {
        const success = await updateJiraTaskStatus(taskId);
        if (success) {
            const displayName = usernameMappings[ctx.from.username] || ctx.from.username;
            await ctx.editMessageText(`Задача ${taskId} взята в работу пользователем ${displayName}\nСсылка: https://jira.sxl.team/browse/${task.key}.`, { reply_markup: { inline_keyboard: [] } });
        } else {
            await ctx.reply(`Не удалось обновить статус задачи ${taskId}. Попробуйте снова.`);
        }
    } else {
        await ctx.editMessageText(`Задача ${taskId} не может быть взята в работу через этот бот. Эта задача для отдела QA`, { reply_markup: { inline_keyboard: [] } });
    }
});

const awareTaskCounts = new Map();

bot.callbackQuery(/^aware_task:(.+)$/, async (ctx) => {
    const taskId = ctx.match[1];
    const username = ctx.from.username;

    if (!awareTaskCounts.has(taskId)) {
        awareTaskCounts.set(taskId, new Set());
    }

    const usersAware = awareTaskCounts.get(taskId);

    if (usersAware.has(username)) {
        return; // User already acknowledged
    }

    usersAware.add(username);

    const task = await getTaskDetails(taskId);
    const department = task.fields.customfield_10500 ? task.fields.customfield_10500.value : 'Не указан';

    let messageText = `Задача: ${task.key}\nСсылка: https://jira.sxl.team/browse/${task.key}\nОписание: ${task.fields.summary}\nПриоритет: ${getPriorityEmoji(task.fields.priority.name)}\nОтдел: ${department}\n\nПользователи в курсе задачи: `;
    messageText += Array.from(usersAware).map(username => usernameMappings[username] || username).join(', ');

    if (usersAware.size >= 3) {
        await ctx.editMessageText(messageText, { reply_markup: { inline_keyboard: [] } });
    } else {
        await ctx.editMessageText(messageText, { reply_markup: ctx.callbackQuery.message.reply_markup });
    }
});

// Обработка команды start
bot.command('start', async (ctx) => {
    await ctx.reply('Привет! Каждую минуту я буду проверять новые задачи.\nВажно: задачи отделов QA и Sportsbook в работу пока брать нельзя, только просматривать');
    await sendJiraTasks(ctx);

    // Установка интервала для регулярной проверки задач
    setInterval(async () => {
        await sendJiraTasks(ctx);
    }, 60000);  // 60000 миллисекунд = 1 минута
});


bot.start();
