FROM node:14

WORKDIR /usr/src/app
# copy only package for caching purposes
COPY src/package*.json ./
RUN yarn install
# copy rest of application
COPY src/src ./src
COPY src/public ./public
# build application
# ARG REMOTE_HOSTNAME="localhost"
# ENV REACT_APP_REMOTE_HOSTNAME "$REMOTE_HOSTNAME"

## Use in development to make build faster:
##
# CMD ["yarn", "start"]

## Use in production:
##
RUN yarn build
CMD ["yarn", "serve"]
