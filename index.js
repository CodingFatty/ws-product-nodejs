require('dotenv').config();
const express = require('express')
const pg = require('pg')
const _ = require('lodash')
const app = express()
// configs come from standard PostgreSQL env vars
// https://www.postgresql.org/docs/9.6/static/libpq-envars.html
const pool = new pg.Pool()

const queryHandler = (req, res, next) => {
  pool.query(req.sqlQuery).then((r) => {
    return res.json(r.rows || [])
  }).catch(next)
}

const limitObj = {
  // Limiting request per Time interval
  secInterval: 1000 * 10, // in 10 seconds
  minInterval: 1000 * 60, // in 60 seconds
  // Number of request per time interval
  secMax: 10,  // default 10 requests in 10 seconds
  minMax: 60,  // default 60 requests in 60 seconds
  // Error message to send when users reach the limit
  errorMsgSec: () => `You reached the ${limitObj.secInterval / 1000} request limits in ${limitObj.secMax} seconds`,
  errorMsgMin: () => `You reached the ${this.minInterval / 1000} request limits in ${this.minMax / 60} minute(s)`
}

const limiter = options => {
  const { secInterval, minInterval, secMax, minMax, errorMsgSec, errorMsgMin } = Object.assign(
    {},
    limitObj,
    options
  )

  let connections = {}

  // Increment counter by key (ip)
  const increment = (key, currentTime) => {
    connections[key] ? null : connections[key] = {
      "secCounter": {},
      "minCounter": {}
    }
    const secKey = Object.keys(connections[key]["secCounter"]).find(key => {
      return key > currentTime - 1000 * 10
    })
    const minKey = Object.keys(connections[key]["minCounter"]).find(key => {
      return key > currentTime - 1000 * 60
    })
    const currentSecVal = connections[key]["secCounter"][secKey] ? connections[key]["secCounter"][secKey] : connections[key]["secCounter"][currentTime] || 0
    const nextSecVal = currentSecVal + 1

    const currentMinVal = connections[key]["minCounter"][minKey] ? connections[key]["minCounter"][minKey] : connections[key]["minCounter"][currentTime] || 0
    const nextMinVal = currentMinVal + 1

    secKey ? connections[key]["secCounter"][secKey] = nextSecVal : connections[key]["secCounter"][currentTime] = nextSecVal
    minKey ? connections[key]["minCounter"][minKey] = nextMinVal : connections[key]["minCounter"][currentTime] = nextMinVal
    // console.log(connections[key])

    // console.log(secKey)
    console.log(`(${JSON.stringify(connections[key], null, 2)}`)

    return nextMinVal
  }

  // Reset Counter when minInterval is passed
  const resetCounters = (key, currentTime) => {
    if (!_.isEmpty(connections[key])) {
      const minKey = Object.keys(connections[key]["minCounter"]).find(key => {
        return key > currentTime - 1000 * 60
      })
      minKey ? null : connections[key] = {
        "secCounter": {},
        "minCounter": {}
      }
    }
  }

  // Rejecting request
  const rejectRequest = (res, type, resetTime) => {
    res.setHeader('X-RateLimit-Reset', Math.ceil(resetTime / 1000))
    if (type === 'sec') {
      return res.status(429).json({ message: errorMsgSec() })
    } else {
      return res.status(429).json({ message: errorMsgMin() })
    }

  }

  const checkLimit = (res, key, currentTime) => {
    if (!_.isEmpty(connections[key])) {
      const secKey = Object.keys(connections[key]["secCounter"]).find(key => {
        return key > currentTime - 1000 * 10
      })
      const minKey = Object.keys(connections[key]["minCounter"]).find(key => {
        return key > currentTime - 1000 * 60
      })
      currentSecCount = connections[key]["secCounter"][secKey]
      currentMinCount = connections[key]["minCounter"][minKey]
      // console.log(currentSecCount)
      if (currentSecCount >= secMax) {
        //reject
        res.setHeader('X-RateLimit-Limit', secMax)
        res.setHeader('X-RateLimit-Remaining', 0)
        return rejectRequest(res, 'sec', currentSecCount + secInterval)
      }
      if (currentMinCount >= minMax) {
        //reject
        res.setHeader('X-RateLimit-Limit', minMax)
        res.setHeader('X-RateLimit-Remaining', 0)
        return rejectRequest(res, 'min', currentMinCount + minInterval)
      }
    }
  }

  return (req, res, next) => {
    const key = req.ip
    const currentTime = Date.now();

    // if minInterval is passed, reset all Counters
    resetCounters(key, currentTime);

    // check if user reaches any limit
    if (checkLimit(res, key, currentTime)) {
      return true
    };

    // process the request
    const count = increment(key, currentTime)

    // return the remaining limit
    res.setHeader('X-RateLimit-Limit', minMax)
    res.setHeader('X-RateLimit-Remaining', minMax - count)

    next()
  }
}

app.use(limiter())

app.get('/', (req, res) => {
  res.send('Welcome to EQ Works 😎')
})

app.get('/events/hourly', (req, res, next) => {
  req.sqlQuery = `
    SELECT date, hour, events
    FROM public.hourly_events
    ORDER BY date, hour
    LIMIT 168;
  `
  return next()
}, queryHandler)

app.get('/events/daily', (req, res, next) => {
  req.sqlQuery = `
    SELECT date, SUM(events) AS events
    FROM public.hourly_events
    GROUP BY date
    ORDER BY date
    LIMIT 7;
  `
  return next()
}, queryHandler)

app.get('/stats/hourly', (req, res, next) => {
  req.sqlQuery = `
    SELECT date, hour, impressions, clicks, revenue
    FROM public.hourly_stats
    ORDER BY date, hour
    LIMIT 168;
  `
  return next()
}, queryHandler)

app.get('/stats/daily', (req, res, next) => {
  req.sqlQuery = `
    SELECT date,
        SUM(impressions) AS impressions,
        SUM(clicks) AS clicks,
        SUM(revenue) AS revenue
    FROM public.hourly_stats
    GROUP BY date
    ORDER BY date
    LIMIT 7;
  `
  return next()
}, queryHandler)

app.get('/poi', (req, res, next) => {
  req.sqlQuery = `
    SELECT *
    FROM public.poi;
  `
  return next()
}, queryHandler)

app.listen(process.env.PORT || 5555, (err) => {
  if (err) {
    console.error(err)
    process.exit(1)
  } else {
    console.log(`Running on ${process.env.PORT || 5555}`)
  }
})

// last resorts
process.on('uncaughtException', (err) => {
  console.log(`Caught exception: ${err}`)
  process.exit(1)
})
process.on('unhandledRejection', (reason, p) => {
  console.log('Unhandled Rejection at: Promise', p, 'reason:', reason)
  process.exit(1)
})
