TARGET      = rlm_mongodb
SRCS        = rlm_mongodb.c
RLM_CFLAGS  = -I/usr/include/mongo-client -I/usr/include/glib-2.0 -I/usr/lib/x86_64-linux-gnu/glib-2.0/include 
RLM_LDFLAGS = -lmongo-client

include ../rules.mak
