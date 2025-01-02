# retrieval-http

## 构建服务
```
make
```

## 设置环境变量
```bash
export MYSQL_DSN="user:password@tcp(127.0.0.1:3306)/dbname?charset=utf8mb4&parseTime=True&loc=Local"
export PORT="8080"
```

## 启动服务
```bash
./retrieval-http
```

## 测试接口
```bash
curl -v http://localhost:8080/retrieval/<pieceCid>
```

### 响应情况
- **200 OK**：返回指定 CAR 文件内容。
- **404 Not Found**：pieceCid 不存在于数据库中。
- **500 Internal Server Error**：文件或数据库读取时出现其他错误。
