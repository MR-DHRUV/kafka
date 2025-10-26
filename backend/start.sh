set -e

node --watch e:/kafka/backend/analytic-service/index.js &
node --watch e:/kafka/backend/email-service/index.js &
node --watch e:/kafka/backend/order-service/index.js &
node --watch e:/kafka/backend/payment-service/index.js &

wait
