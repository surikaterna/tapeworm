FROM node:26.9.0-bookworm
RUN npm install --global npm@11.12.1 \
    && test "$(node --version)" = v26.9.0 \
    && test "$(npm --version)" = 11.12.1 \
    && dpkg-query -W libcurl4
