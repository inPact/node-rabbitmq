FROM rabbitmq:3.8-management

ARG DELAY_PLUGIN_VERSION=3.8.0

RUN apt-get update \
    && apt-get -y install --no-install-recommends ca-certificates vim curl wget \
    && apt-get clean -y

WORKDIR /opt/rabbitmq/plugins/

RUN wget -q https://github.com/rabbitmq/rabbitmq-delayed-message-exchange/releases/download/v${DELAY_PLUGIN_VERSION}/rabbitmq_delayed_message_exchange-${DELAY_PLUGIN_VERSION}.ez

RUN rabbitmq-plugins enable rabbitmq_delayed_message_exchange
