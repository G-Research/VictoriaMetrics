ARG base_image=non-existing
FROM $base_image

EXPOSE 8402

ENTRYPOINT ["/vmread-prod"]
ARG src_binary=non-existing
COPY $src_binary ./vmread-prod

