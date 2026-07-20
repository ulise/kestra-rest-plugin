# Build with:
#   ./gradlew shadowJar && docker build -t kestra-with-rest-plugin .
#
# The image must expose the plugin's port (8090) alongside Kestra's own (8080).
FROM kestra/kestra:latest

COPY build/libs/plugin-rest-server-*.jar /app/plugins/

EXPOSE 8080 8090
