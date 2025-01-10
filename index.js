require('dotenv').config();
const { Bot, InlineKeyboard } = require('grammy');
const { conversations, createConversation } = require('@grammyjs/conversations');
const axios = require('axios');
const sqlite3 = require('sqlite3').verbose();
const { DateTime } = require('luxon');
const cron = require('node-cron');

const bot = new Bot(process.env.BOT_API_KEY);
bot.use(conversations()); // Подключаем middleware для conversation

const db = new sqlite3.Database('tasks.db');

/**
 * Функция возвращает дату-время по Москве 'yyyy-MM-dd HH:mm:ss'.
 */
function getMoscowTimestamp() {
    return DateTime.now().setZone('Europe/Moscow').toFormat('yyyy-MM-dd HH:mm:ss');
}

/**
 * Создаём нужные таблицы (если их нет).
 * Архивирование (archived=1) + archivedDate — чтобы не ломать отчёты.
 */
db.run(`
  CREATE TABLE IF NOT EXISTS tasks (
    id TEXT PRIMARY KEY,
    title TEXT,
    priority TEXT,
    department TEXT,
    issueType TEXT,
    resolution TEXT,
    assignee TEXT,
    dateAdded DATETIME,
    lastSent DATETIME,
    source TEXT,
    archived INTEGER DEFAULT 0,
    archivedDate DATETIME
)
`);

db.run(`
  CREATE TABLE IF NOT EXISTS user_actions (
    username TEXT,
    taskId TEXT,
    action TEXT,
    timestamp DATETIME,
    FOREIGN KEY(taskId) REFERENCES tasks(id)
)
`);

db.run(`
  CREATE TABLE IF NOT EXISTS task_comments (
    taskId TEXT,
    lastCommentId TEXT,
    timestamp DATETIME,
    FOREIGN KEY(taskId) REFERENCES tasks(id)
)
`);

/**
 * Привязка Telegram username -> ФИО, а также Jira-логины.
 * Например:
 *  - userMappings[tgUsername].name => ФИО
 *  - userMappings[tgUsername].sxl  => логин Jira SXL
 *  - userMappings[tgUsername].betone => логин Jira BetOne
 */
const userMappings = {
    lipchinski: {
        name: "Дмитрий Селиванов",
        sxl: "d.selivanov",
        betone: "dms"
    },
    pr0spal: {
        name: "Евгений Шушков",
        sxl: "e.shushkov",
        betone: "es"
    },
    fdhsudgjdgkdfg: {
        name: "Даниил Маслов",
        sxl: "d.maslov",
        betone: "dam"
    },
    EuroKaufman: {
        name: "Даниил Баратов",
        sxl: "d.baratov",
        betone: "db"
    },
    Nikolay_Gonchar: {
        name: "Николай Гончар",
        sxl: "n.gonchar",
        betone: "ng"
    },
    KIRILlKxX: {
        name: "Кирилл Атанизяов",
        sxl: "k.ataniyazov",
        betone: "ka"
    },
    marysh353: {
        name: "Даниил Марышев",
        sxl: "d.maryshev",
        betone: "dma"
    }
};

/**
 * Функция переводит Telegram username -> ФИО (для отображения).
 */
function mapTelegramUserToName(tgUsername) {
    if (!tgUsername || !userMappings[tgUsername]) return "Неизвестный пользователь";
    return userMappings[tgUsername].name;
}

/**
 * Функция переводит Telegram username -> Jira логин (для конкретного source).
 * Например: getJiraUsername('lipchinski','sxl') => 'd.selivanov'.
 */
function getJiraUsername(tgUsername, source) {
    if (!tgUsername || !userMappings[tgUsername]) return null;
    if (source === 'sxl') return userMappings[tgUsername].sxl;
    if (source === 'betone') return userMappings[tgUsername].betone;
    return null;
}

/**
 * Эмоджи приоритета.
 */
function getPriorityEmoji(priority) {
    const map = {
        Blocker: '🚨',
        High: '🔴',
        Medium: '🟡',
        Low: '🟢'
    };
    return map[priority] || '';
}

/**
 * Основная функция: получить задачи из Jira (sxl и betone).
 */
async function fetchAndStoreJiraTasks() {
    await fetchAndStoreTasksFromJira('sxl','https://jira.sxl.team/rest/api/2/search', process.env.JIRA_PAT_SXL);
    await fetchAndStoreTasksFromJira('betone','https://jira.betone.team/rest/api/2/search', process.env.JIRA_PAT_BETONE);
}

/**
 * Получаем задачи (Open, Under review, Wait..., Done).
 * В зависимости от source, поля department могут различаться.
 */
async function fetchAndStoreTasksFromJira(source, url, pat) {
    try {
        const jql = `
            project = SUPPORT
            AND status in ("Open", "Under review", "Waiting for support", "Waiting for Developers approval", "Done")
        `;
        const response = await axios.get(url, {
            headers: {
                Authorization: `Bearer ${pat}`,
                Accept: 'application/json'
            },
            params: { jql }
        });

        const fetchedIssues = response.data.issues || [];
        const fetchedTaskIds = fetchedIssues.map(i => i.key);

        for (const issue of fetchedIssues) {
            const fields = issue.fields;

            let department = 'Не указан';
            if (source === 'sxl') {
                // Например, SXL -> customfield_10500
                department = fields.customfield_10500?.value || 'Не указан';
            } else if (source === 'betone') {
                // BetOne -> customfield_10504
                department = fields.customfield_10504?.value || 'Не указан';
            }

            const resolution = fields.resolution?.name || '';
            const assigneeKey = fields.assignee?.name || '';
            const assigneeMapped = Object.values(userMappings).find(um => um.sxl === assigneeKey || um.betone === assigneeKey);

            const taskData = {
                id: issue.key,
                title: fields.summary,
                priority: fields.priority?.name || 'Не указан',
                issueType: fields.issuetype?.name || 'Не указан',
                department,
                resolution,
                assignee: assigneeMapped ? assigneeMapped.name : '',
                source
            };

            const existing = await new Promise((resolve, reject) => {
                db.get('SELECT * FROM tasks WHERE id=?',[taskData.id],(err,row)=>{
                    if(err) reject(err);
                    else resolve(row);
                });
            });

            if (existing) {
                db.run(
                    `UPDATE tasks
                     SET title=?, priority=?, issueType=?, department=?, resolution=?, assignee=?, source=?, archived=0
                     WHERE id=?`,
                    [
                        taskData.title, 
                        taskData.priority, 
                        taskData.issueType, 
                        taskData.department, 
                        taskData.resolution, 
                        taskData.assignee,
                        taskData.source,
                        taskData.id
                    ]
                );
            } else {
                db.run(
                    `INSERT INTO tasks
                     (id,title,priority,issueType,department,resolution,assignee,dateAdded,lastSent,source,archived,archivedDate)
                     VALUES(?,?,?,?,?,?,?,?,NULL,?,0,NULL)`,
                    [
                        taskData.id,
                        taskData.title,
                        taskData.priority,
                        taskData.issueType,
                        taskData.department,
                        taskData.resolution,
                        taskData.assignee,
                        getMoscowTimestamp(),
                        taskData.source
                    ]
                );
            }
        }

        // Архивируем те, что не пришли
        if(fetchedTaskIds.length>0){
            const placeholders = fetchedTaskIds.map(()=>'?').join(',');
            db.run(
                `UPDATE tasks
                 SET archived=1, archivedDate=?
                 WHERE source=?
                   AND archived=0
                   AND id NOT IN (${placeholders})`,
                [getMoscowTimestamp(), source, ...fetchedTaskIds]
            );
        } else {
            // Вообще нет задач => все архивируем
            db.run(
                `UPDATE tasks
                 SET archived=1, archivedDate=?
                 WHERE source=?
                   AND archived=0`,
                [getMoscowTimestamp(), source]
            );
        }
    } catch(e) {
        console.error(`fetchAndStoreTasksFromJira(${source}) error:`, e);
    }
}

/**
 * Отправляет задачи, если:
 *  - department = "Техническая поддержка" => lastSent < cегодня
 *  - issueType in (Infra,Office,Prod) => lastSent < now - 3 days
 */
async function sendJiraTasks(ctx) {
    const today = getMoscowTimestamp().split(' ')[0];
    // В SQL делаем объединённое условие:
    // (dep=Техподдержка + 1 день) OR (issueType in ... + 3 дня)
    // При этом упорядочим так, чтобы Техподдержка шла первым блоком.
    const query = `
      SELECT *
      FROM tasks
      WHERE archived=0
        AND (
          (
            department='Техническая поддержка'
            AND (lastSent IS NULL OR lastSent < date('${today}'))
          )
          OR
          (
            issueType IN ('Infra','Office','Prod')
            AND (lastSent IS NULL OR lastSent < datetime('now','-3 days'))
          )
        )
      ORDER BY CASE WHEN department='Техническая поддержка' THEN 1 ELSE 2 END
    `;

    db.all(query, [], async (err, rows) => {
        if (err) {
            console.error('sendJiraTasks() error:', err);
            return;
        }
        for (const task of rows) {
            // Формируем инлайн-кнопки
            const kb = new InlineKeyboard();
            const jiraUrl = `https://jira.${task.source}.team/browse/${task.id}`;

            // Для Техподдержки: 3 кнопки
            if (task.department === 'Техническая поддержка') {
                kb
                  .text('Взять в работу', `take_task:${task.id}`)
                  .text('Комментарий', `comment_task:${task.id}`)
                  .text('Завершить', `complete_task:${task.id}`)
                  .row()
                  .url('Перейти к задаче', jiraUrl);
            // Для Infra / Office / Prod: одна кнопка
            } else if(['Infra','Office','Prod'].includes(task.issueType)) {
                kb.url('Перейти к задаче', jiraUrl);
            } else {
                kb.url('Перейти к задаче', jiraUrl);
            }

            const msgText = `
Задача: ${task.id}
Источник: ${task.source}
Описание: ${task.title}
Приоритет: ${getPriorityEmoji(task.priority)}
Тип задачи: ${task.issueType}
Исполнитель: ${task.assignee || 'не назначен'}
Департамент: ${task.department}
            `.trim();

            const sent = await ctx.reply(msgText, { reply_markup: kb });
            // Обновляем lastSent
            db.run('UPDATE tasks SET lastSent=? WHERE id=?',[getMoscowTimestamp(), task.id]);

            // Возможно, вы захотите сохранить message_id в БД, чтобы при редактировании этого сообщения знать, что именно редактировать.
            // Для упрощения тут не сохраняем, а будем работать через callbackQuery.message при нажатии кнопок.
        }
    });
}

/* 
  Ниже блок "интерактивных" функций для работы с Jira API 
  (взять в работу=assignee, добавить комментарий, завершить=transition)
*/

/**
 * Назначить задачу на пользователя (assignee).
 * Например, PUT /rest/api/2/issue/{taskId}/assignee
 */
async function updateJiraAssignee(source, taskId, jiraUsername) {
    try {
        const url = `https://jira.${source}.team/rest/api/2/issue/${taskId}/assignee`;
        const pat = (source === 'sxl') ? process.env.JIRA_PAT_SXL : process.env.JIRA_PAT_BETONE;

        // Jira требует JSON: { name: "d.selivanov" } или { accountId: ... } в Cloud. 
        // В Server/DC — обычно { name: "..." }.
        const resp = await axios.put(url, { name: jiraUsername }, {
            headers: {
                Authorization: `Bearer ${pat}`,
                Accept: 'application/json'
            }
        });
        console.log(`Assignee updated for ${taskId}:`, resp.status);
        return true;
    } catch(e) {
        console.error(`updateJiraAssignee error:`, e.response?.data || e);
        return false;
    }
}

/**
 * Добавить комментарий к задаче.
 * POST /rest/api/2/issue/{taskId}/comment
 */
async function updateJiraIssueComment(source, taskId, jiraUsername, commentBody) {
    try {
        const url = `https://jira.${source}.team/rest/api/2/issue/${taskId}/comment`;
        const pat = (source === 'sxl') ? process.env.JIRA_PAT_SXL : process.env.JIRA_PAT_BETONE;

        // Пример: { body: "Сделано то-то." }
        // Если нужно упоминать автора — Jira сама знает, кто автор (через PAT),
        // но иногда приходится имитировать. Зависит от настроек.
        const resp = await axios.post(url, { body: commentBody }, {
            headers: {
                Authorization: `Bearer ${pat}`,
                Accept: 'application/json'
            }
        });
        console.log(`Comment added for ${taskId}:`, resp.status);
        return true;
    } catch(e) {
        console.error('updateJiraIssueComment error:', e.response?.data || e);
        return false;
    }
}

/**
 * Перевести задачу в нужный статус (например, Done).
 * POST /rest/api/2/issue/{taskId}/transitions
 */
async function updateJiraTaskStatus(source, taskId, transitionId) {
    try {
        const url = `https://jira.${source}.team/rest/api/2/issue/${taskId}/transitions`;
        const pat = (source === 'sxl') ? process.env.JIRA_PAT_SXL : process.env.JIRA_PAT_BETONE;

        // Jira ожидает { transition: { id: "xxx" } }
        const resp = await axios.post(url, { transition: { id: transitionId } }, {
            headers: {
                Authorization: `Bearer ${pat}`,
                Accept: 'application/json'
            }
        });
        console.log(`Status updated for ${taskId}:`, resp.status);
        return true;
    } catch(e) {
        console.error('updateJiraTaskStatus error:', e.response?.data || e);
        return false;
    }
}

/* 
  Теперь пишем conversation для "Добавить комментарий".
  Conversation спрашивает у пользователя: "Введи комментарий".
  После ответа - отправляем в Jira. 
*/
async function commentConversation(conversation, ctx) {
    // 1. Получаем из callback data taskId и source
    const parts = ctx.match.input.split(':'); // "comment_task:ABC-123"
    // parts[0] = "comment_task", parts[1] = "ABC-123"
    const taskId = parts[1];

    // 2. Нужно узнать, из какой Jira эта задача. Для упрощения возьмём из DB.
    const taskRow = await new Promise((resolve, reject) => {
        db.get('SELECT * FROM tasks WHERE id=?',[taskId], (err,row)=>{
            if(err) reject(err);
            else resolve(row);
        });
    });

    if(!taskRow) {
        await ctx.answerCallbackQuery();
        await ctx.reply('Задача не найдена в БД.');
        return;
    }

    const source = taskRow.source;
    const telegramUsername = ctx.from?.username || '';
    const realName = mapTelegramUserToName(telegramUsername);
    const jiraUsername = getJiraUsername(telegramUsername, source);

    if(!jiraUsername) {
        await ctx.answerCallbackQuery();
        await ctx.reply(`Не найден Jira-логин для пользователя ${telegramUsername}`);
        return;
    }

    // 3. Запрашиваем комментарий
    await ctx.answerCallbackQuery();
    await ctx.reply('Введите комментарий для задачи:');
    const { message } = await conversation.wait();

    const userComment = message.text; // То, что пользователь ввёл

    // 4. Шлём комментарий в Jira
    const success = await updateJiraIssueComment(source, taskId, jiraUsername, userComment);

    if(!success) {
        await ctx.reply('Ошибка при добавлении комментария в Jira.');
        return;
    }

    // 5. Редактируем исходное сообщение
    // conversation.wait() уводит нас из контекста callbackQuery, 
    // но мы можем попробовать сохранить message_id до входа в conversation 
    // Либо, передав ctx.callbackQuery.message.message_id в "meta".
    // Упрощённо: попытаемся ctx.editMessageText, если есть callbackQuery в начале.
    const callbackMsg = ctx.callbackQuery?.message;
    if(callbackMsg) {
        try {
            await bot.api.editMessageText(
                callbackMsg.chat.id,
                callbackMsg.message_id,
                `${taskRow.department}\n\nКомментарий добавлен: ${realName}`
            );
        } catch(e) {
            console.error('editMessageText (comment) error:', e);
        }
    } else {
        // Если не получилось, просто отправим новое сообщение
        await ctx.reply(`${taskRow.department}\n\nКомментарий добавлен: ${realName}`);
    }
}

/** 
 * Регистрируем conversation "commentConversation"
 */
bot.use(createConversation(commentConversation, "commentConversation"));

/**
 * CallbackQuery обрабатываем тремя вариантами:
 *  - take_task => назначить assignee + editMessage
 *  - comment_task => conversation (запрос комментария), после чего editMessage
 *  - complete_task => transition=401 => editMessage
 */
bot.callbackQuery(/^(take_task|comment_task|complete_task):(.*)$/, async (ctx) => {
    const actionType = ctx.match[1];
    const taskId = ctx.match[2];
    const telegramUsername = ctx.from?.username || '';
    const realName = mapTelegramUserToName(telegramUsername);

    // Смотрим задачу в БД, чтобы понять, из какой Jira
    const taskRow = await new Promise((resolve, reject)=>{
        db.get('SELECT * FROM tasks WHERE id=?',[taskId], (err,row)=>{
            if(err) reject(err);
            else resolve(row);
        });
    });

    if(!taskRow) {
        await ctx.answerCallbackQuery();
        await ctx.reply('Задача не найдена в БД.');
        return;
    }
    const { source, department } = taskRow;
    const jiraUsername = getJiraUsername(telegramUsername, source);

    if(!jiraUsername) {
        await ctx.answerCallbackQuery();
        await ctx.reply(`Не найден Jira-логин для пользователя ${telegramUsername}`);
        return;
    }

    if(actionType === 'take_task') {
        // Взять в работу = assignee
        const ok = await updateJiraAssignee(source, taskId, jiraUsername);
        await ctx.answerCallbackQuery();
        if(ok) {
            // Редактируем исходное сообщение
            try {
                await ctx.editMessageText(`${department}\n\nВзял в работу: ${realName}`);
            } catch(e) {
                console.error('editMessageText(take_task) error:', e);
            }
        } else {
            await ctx.reply('Ошибка назначения исполнителя в Jira.');
        }

    } else if(actionType === 'comment_task') {
        // Запускаем conversation
        // conversation внутри callbackQuery нужно вызывать через ctx.conversation.enter(...)
        await ctx.conversation.enter("commentConversation");

    } else if(actionType === 'complete_task') {
        // Завершение => переводим в Done (transitionId=401)
        const transitionId = '401'; 
        const ok = await updateJiraTaskStatus(source, taskId, transitionId);
        await ctx.answerCallbackQuery();
        if(ok) {
            // Пишем "Завершил задачу: {name}"
            try {
                await ctx.editMessageText(`${department}\n\nЗавершил задачу: ${realName}`);
            } catch(e) {
                console.error('editMessageText(complete_task) error:', e);
            }
        } else {
            await ctx.reply('Ошибка при переводе задачи в Done в Jira.');
        }
    }
});

/**
 * /report — статистика по задачам (Done, Техподдержка) за 30 дней (учитывая archived=0/1).
 */
bot.command('report', async (ctx) => {
    try {
        const dateLimit = DateTime.now().setZone('Europe/Moscow').minus({ days:30 }).toFormat('yyyy-MM-dd');
        const sql = `
          SELECT assignee FROM tasks
          WHERE resolution='Done'
            AND department='Техническая поддержка'
            AND date(dateAdded) >= date(?)
        `;
        db.all(sql, [dateLimit], async (err, rows)=>{
            if(err) {
                console.error('/report DB error:', err);
                await ctx.reply('Ошибка при формировании отчёта.');
                return;
            }
            if(!rows || rows.length===0) {
                await ctx.reply('За последние 30 дней нет выполненных задач в Техподдержке.');
                return;
            }
            const stats={};
            for(const r of rows) {
                const name = r.assignee || 'Неизвестный';
                if(!stats[name]) stats[name]=0;
                stats[name]++;
            }
            let msg='Отчёт по выполненным задачам (Техподдержка) за последние 30 дней:\n\n';
            for(const n of Object.keys(stats)) {
                msg += `${n}: ${stats[n]} задач(и)\n`;
            }
            await ctx.reply(msg);
        });
    } catch(e) {
        console.error('/report error:', e);
        await ctx.reply('Ошибка при формировании отчёта.');
    }
});

/**
 * Проверка новых комментариев в закрытых задачах (Done) в Техподдержке, каждые 5 мин.
 * (Логика осталась как пример. Если надо — оставьте, если нет — уберите.)
 */
async function checkNewCommentsInDoneTasks() {
    try {
        const sql = `
          SELECT * 
          FROM tasks
          WHERE department='Техническая поддержка'
            AND resolution='Done'
            AND archived=0
        `;
        db.all(sql, [], async (err, tasks)=>{
            if(err) {
                console.error('checkNewCommentsInDoneTasks DB error:', err);
                return;
            }
            for(const task of tasks) {
                const { id, source } = task;
                const tableCommentInfo = await new Promise((resolve,reject)=>{
                    db.get('SELECT * FROM task_comments WHERE taskId=?',[id], (err2,row)=>{
                        if(err2) reject(err2);
                        else resolve(row);
                    });
                });
                const lastSavedCommentId = tableCommentInfo?.lastCommentId || null;
                // Запрашиваем комментарии
                const url = `https://jira.${source}.team/rest/api/2/issue/${id}/comment`;
                const pat = (source==='sxl')? process.env.JIRA_PAT_SXL: process.env.JIRA_PAT_BETONE;
                const resp = await axios.get(url,{
                    headers:{
                        Authorization:`Bearer ${pat}`,
                        Accept:'application/json'
                    }
                });
                const comments = resp.data.comments || [];
                comments.sort((a,b)=> parseInt(a.id)-parseInt(b.id));
                
                let newLastId = lastSavedCommentId;
                for(const c of comments) {
                    const cId = parseInt(c.id);
                    const lastIdNum = lastSavedCommentId ? parseInt(lastSavedCommentId) : 0;
                    if(cId>lastIdNum) {
                        // Новый комментарий
                        // Отправляем сообщение (или нет) - по желанию
                        // Здесь пример
                        const authorName = c.author?.displayName||'Неизвестный автор';
                        const bodyText = c.body||'';
                        const msgText=`
В выполненную задачу добавлен новый комментарий

Задача: ${task.id}
Источник: ${task.source}
Описание: ${task.title}
Приоритет: ${getPriorityEmoji(task.priority)}
Тип задачи: ${task.issueType}

Автор: ${authorName}
Комментарий: ${bodyText}
                        `.trim();
                        await bot.api.sendMessage(process.env.ADMIN_CHAT_ID, msgText);
                        if(!newLastId || cId> parseInt(newLastId)) newLastId=c.id;
                    }
                }
                // Обновим lastCommentId
                if(newLastId && newLastId!==lastSavedCommentId){
                    if(tableCommentInfo) {
                        db.run(`UPDATE task_comments SET lastCommentId=?, timestamp=? WHERE taskId=?`,
                            [newLastId, getMoscowTimestamp(), id]);
                    } else {
                        db.run(`INSERT INTO task_comments (taskId,lastCommentId,timestamp) VALUES(?,?,?)`,
                            [id,newLastId,getMoscowTimestamp()]);
                    }
                }
            }
        });
    } catch(e) {
        console.error('checkNewCommentsInDoneTasks error:', e);
    }
}

/**
 * Ежедневная очистка старых задач (Done+archived=1, >35 дней).
 */
cron.schedule('0 0 * * *', ()=>{
    console.log('Daily cleanup started...');
    const cleanup = `
      DELETE FROM tasks
      WHERE archived=1
        AND resolution='Done'
        AND archivedDate IS NOT NULL
        AND date(archivedDate)<date('now','-35 days')
    `;
    db.run(cleanup, function(err){
        if(err) console.error('Cleanup error:', err);
        else console.log(`Cleaned ${this.changes} tasks`);
    });

    // Чистим actions/comments
    const cleanActions=`DELETE FROM user_actions WHERE taskId NOT IN (SELECT id FROM tasks)`;
    db.run(cleanActions);

    const cleanComments=`DELETE FROM task_comments WHERE taskId NOT IN (SELECT id FROM tasks)`;
    db.run(cleanComments);
});

/**
 * Раз в 5 минут - проверяем новые комментарии (Done, Техподдержка).
 */
cron.schedule('*/5 * * * *', async()=>{
    console.log('checkNewCommentsInDoneTasks running...');
    await checkNewCommentsInDoneTasks();
});

/**
 * Команда /start: приветствие + периодическая проверка задач.
 */
bot.command('start', async(ctx)=>{
    await ctx.reply(
        'Привет! Я буду сообщать о новых задачах.\n'+
        'Используй /report для отчёта по выполненным задачам.'
    );
    fetchAndStoreJiraTasks().then(()=> sendJiraTasks(ctx));

    // Каждые 2 минуты проверяем новые/обновлённые задачи
    cron.schedule('*/2 * * * *', async()=>{
        console.log('Fetching tasks...');
        await fetchAndStoreJiraTasks();
        await sendJiraTasks(ctx);
    });
});

/**
 * Утренние/ночные уведомления (пример).
 */
cron.schedule('0 21 * * *', async()=>{
    await bot.api.sendMessage(process.env.ADMIN_CHAT_ID, 'Доброй ночи! Заполни тикет передачи смены.');
});
cron.schedule('0 9 * * *', async()=>{
    await bot.api.sendMessage(process.env.ADMIN_CHAT_ID, 'Доброе утро! Проверь задачи на сегодня и начни смену.');
});

/**
 * Запускаем бота.
 */
bot.start();
