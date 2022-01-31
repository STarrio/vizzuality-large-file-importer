FROM node:16

WORKDIR /usr/src/app
COPY package*.json ./

RUN npm install

EXPOSE 9999

COPY . .

ENTRYPOINT npm run start
