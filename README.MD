Для бота необходимо:

1. sudo apt-get update && sudo apt-get upgrade -y

2. sudo apt install git
3. git clone https://github.com/zverstal/JiraTelegramJS

4. sudo apt-get install -y ca-certificates curl gnupg
5. sudo mkdir -p /etc/apt/keyrings
6. curl -fsSL https://deb.nodesource.com/gpgkey/nodesource-repo.gpg.key | sudo gpg --dearmor -o /etc/apt/keyrings/nodesource.gpg

7. NODE_MAJOR=21
echo "deb [signed-by=/etc/apt/keyrings/nodesource.gpg] https://deb.nodesource.com/node_$NODE_MAJOR.x nodistro main" | sudo tee /etc/apt/sources.list.d/nodesource.list

8. 
sudo apt-get update
sudo apt-get install nodejs -y

9. npm i

10. 
touch .env
nano .env:
BOT_API_KEY='API_ключ_бота'
JIRA_PAT='API_ключ_Jira'

11. npm i pm2 -g

12. pm2 start index.js