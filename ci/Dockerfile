FROM node:26.12.1-bookworm
RUN npm install --global npm@11.12.1 \
    && test "$(node --version)" = v26.12.1 \
    && test "$(npm --version)" = 11.12.1 \
    && dpkg-query -W libcurl4
