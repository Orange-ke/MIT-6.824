package middleware

import (
	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"time"
)

// 跨域中间件

func Cors() gin.HandlerFunc {
	return cors.New(cors.Config{
		AllowAllOrigins: true,
		//AllowOrigins:     []string{"*"},
		AllowMethods:     []string{"*"},
		ExposeHeaders:    []string{"Content-Type", "Authorization", "content-Length"},
		AllowHeaders:     []string{"*"},
		AllowCredentials: true,           // 是不是发送cookie
		MaxAge:           12 * time.Hour, // 保存与请求权限的时间
	})
}
