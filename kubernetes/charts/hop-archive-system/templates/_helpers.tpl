{{- define "archiver.env" }}

- name: POSTGRESQL_HOST
  value: {{ .Values.archiveDb.fullnameOverride }}
- name: POSTGRESQL_USERNAME
  value: {{ .Values.archiveDb.global.postgresql.auth.username }}
- name: POSTGRESQL_DATABASE
  value: {{ .Values.archiveDb.global.postgresql.auth.database }}
- name: POSTGRESQL_PASSWORD
  valueFrom:
    secretKeyRef:
      name: {{ .Values.archiveDb.global.postgresql.auth.existingSecret }}
      key: {{ .Values.archiveDb.global.postgresql.auth.secretKeys.userPasswordKey }}

- name: HOP_USERNAME
  valueFrom:
    secretKeyRef:
      name: {{ .Values.archiver.hop.auth.existingSecret }}
      key: {{ .Values.archiver.hop.auth.secretKeys.userNameKey }}
- name: HOP_PASSWORD
  valueFrom:
    secretKeyRef:
      name: {{ .Values.archiver.hop.auth.existingSecret }}
      key: {{ .Values.archiver.hop.auth.secretKeys.userPasswordKey }}


- name: MONGO_INITDB_ROOT_USERNAME
  value: root
- name: MONGO_INITDB_ROOT_PASSWORD
  valueFrom:
    secretKeyRef:
      name: {{ .Values.mongoDb.auth.existingSecret }}
      key: "mongodb-root-password"

{{- end }}


{{- define "api.env" }}

- name: POSTGRESQL_HOST
  value: {{ .Values.archiveApiDb.fullnameOverride }}
- name: POSTGRESQL_USERNAME
  value: {{ .Values.archiveApiDb.global.postgresql.auth.username }}
- name: POSTGRESQL_DATABASE
  value: {{ .Values.archiveApiDb.global.postgresql.auth.database }}
- name: POSTGRESQL_PASSWORD
  valueFrom:
    secretKeyRef:
      name: {{ .Values.archiveApiDb.global.postgresql.auth.existingSecret }}
      key: {{ .Values.archiveApiDb.global.postgresql.auth.secretKeys.userPasswordKey }}

- name: ARCHIVE_DB_HOST
  value: {{ .Values.archiveDb.fullnameOverride }}
- name: ARCHIVE_DB_USERNAME
  value: {{ .Values.archiveDb.global.postgresql.auth.username }}
- name: ARCHIVE_DB_DATABASE
  value: {{ .Values.archiveDb.global.postgresql.auth.database }}
- name: ARCHIVE_DB_PASSWORD
  valueFrom:
    secretKeyRef:
      name: {{ .Values.archiveDb.global.postgresql.auth.existingSecret }}
      key: {{ .Values.archiveDb.global.postgresql.auth.secretKeys.userPasswordKey }}

- name: DJANGO_DEBUG
  value: {{ .Values.archiveApi.debug | quote }}
- name: DJANGO_SECRET_KEY
  valueFrom:
    secretKeyRef:
      name: {{ .Values.archiveApi.django.secretKey.existingSecret }}
      key: {{ .Values.archiveApi.django.secretKey.secretKey }}
  value: 1234
- name: DJANGO_HOSTNAME
  value: {{ .Values.archiveApi.ingress.hostname }}
- name: DJANGO_URL_BASE_PATH
  value: {{ .Values.archiveApi.ingress.basePath }}
- name: DJANGO_SUPERUSER_DATABASE
  value: default
- name: DJANGO_SUPERUSER_EMAIL
  value: {{ .Values.archiveApi.django.admin.email }}
- name: DJANGO_SUPERUSER_USERNAME
  value: {{ .Values.archiveApi.django.admin.auth.username }}
- name: DJANGO_SUPERUSER_PASSWORD
  valueFrom:
    secretKeyRef:
      name: {{ .Values.archiveApi.django.admin.auth.existingSecret }}
      key: {{ .Values.archiveApi.django.admin.auth.secretKeys.adminPasswordKey }}

- name: S3_ACCESS_KEY_ID
  value: minioadmin
- name: S3_SECRET_ACCESS_KEY
  value: minioadmin

- name: MONGO_DB_HOSTNAME
  value: {{ .Values.mongoDb.fullnameOverride }}
- name: MONGO_DB_USERNAME
  value: root
- name: MONGO_INITDB_ROOT_USERNAME
  value: root
- name: MONGO_DB_PASSWORD
  valueFrom:
    secretKeyRef:
      name: {{ .Values.mongoDb.auth.existingSecret }}
      key: "mongodb-root-password"
- name: MONGO_INITDB_ROOT_PASSWORD
  valueFrom:
    secretKeyRef:
      name: {{ .Values.mongoDb.auth.existingSecret }}
      key: "mongodb-root-password"
- name: MONGO_DB_DATABASE
  value: {{ index .Values.mongoDb.auth.databases 0 }}
- name: MONGO_DB_COLLECTION
  value: {{ .Values.mongoDb.collection }}
- name: MONGO_DB_PORT
  value: "27017"
{{- if eq .Values.archiveApi.debug "true" }}
- name: LOG_LEVEL
  value: DEBUG
{{- end }}
{{- end }}

{{- define "browser.env" }}
- name: REACT_APP_REMOTE_PROTOCOL
  value: "https"
- name: REACT_APP_REMOTE_HOSTNAME
  value: {{ .Values.archiveBrowser.ingress.hostname }}
- name: REACT_APP_REMOTE_PORT
  value: "80"
- name: REACT_APP_REMOTE_BASEPATH
  value:  {{ .Values.archiveApi.ingress.basePath }}
{{- end }}


