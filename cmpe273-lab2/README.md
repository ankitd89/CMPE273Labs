cmpe273-lab2
============

Node.JS with [Connect]

### To Install Dependencies ###

```sh
npm install
```

### To run the application
```sh
node index.js
```

### cURL commands

* GET
```sh
curl -i http://localhost:8000/ -X GET
```

* GET with setting **Cookies** in the request header.
```sh
curl -i http://localhost:8000/ -X GET --cookie "session_id=99999"
# Add -v to enable the verbose mode to see what you are sending in the request header.
curl -i http://localhost:8000/ -X GET --cookie "session_id=99999" -v
```

* POST with JSON payload in the request body.
```sh
curl -i http://localhost:8000/ -X POST -H "Content-Type: application/json" -d '{ "name" : "Foo" }'
```

* PUT with an optional cookie
```sh
curl -i http://localhost:8000/ -X PUT --cookie "session_id=xxxxxxxxxx"
```

* DELETE with an optional cookie
```sh
curl -i http://localhost:8000/ -X DELETE --cookie "session_id=xxxxxxxxxx"
```

[Connect]:http://www.senchalabs.org/connect/