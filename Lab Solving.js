// Help MongoDB pick a movie our next movie night! Based on employee polling, we've decided that potential movies must meet the following criteria.

// Problem 1
// imdb.rating is at least 7
// genres does not contain "Crime" or "Horror"
// rated is either "PG" or "G"
// languages contains "English" and "Japanese"
// Assign the aggregation to a variable named pipeline
var pipeline = [
  {
    $match: {
      "imdb.rating": { $gte: 7 },
      genres: { $nin: ["Crime", "Horror"] },
      rated: { $in: ["PG", "G"] },
      languages: { $all: ["English", "Japanese"] },
    },
  },
];

// Problem 2
// Our first movie night was a success. Unfortunately, our ISP called to let us know we're close to our bandwidth quota, but we need another movie recommendation!
// Using the same $match stage from the previous lab, add a $project stage to only display the title and film rating (title and rated fields).
// Assign the results to a variable called pipeline.
var pipeline = [
  {
    $match: {
      "imdb.rating": { $gte: 7 },
      genres: { $nin: ["Crime", "Horror"] },
      rated: { $in: ["PG", "G"] },
      languages: { $all: ["English", "Japanese"] },
    },
  },
  {
    $project: {
      _id: 0,
      title: 1,
      rated: 1,
    },
  },
];

// Problem 3
// Our movies dataset has a lot of different documents, some with more convoluted titles than others. If we'd like to analyze our collection to find movie titles that are composed of only one word, we could fetch all the movies in the dataset and do some processing in a client application, but the Aggregation Framework allows us to do this on the server!
// Using the Aggregation Framework, find a count of the number of movies that have a title composed of one word. To clarify, "Cinderella" and "3-25" should count, where as "Cast Away" would not.
// Make sure you look into the $split String expression and the $size Array expression
// To get the count, you can append itcount() to the end of your pipeline
db.movies
  .aggregate([{ $match: { title: { $not: { $regex: /[\s]/ } } } }])
  .itcount();
// low performance, but less code
// OR
db.movies
  .aggregate([
    {
      $match: {
        title: {
          $type: "string",
        },
      },
    },
    {
      $project: {
        title: { $split: ["$title", " "] },
        _id: 0,
      },
    },
    {
      $match: {
        title: { $size: 1 },
      },
    },
  ])
  .itcount();
// high performance, but more code
// explanation
// 1. match only documents with title as string
// 2. project title as array of words
// 3. match only documents with array of words with size 1

// Problem 4
// Let's find how many movies in our movies collection are a "labor of love", where the same person appears in cast, directors, and writers
// Hint: You will need to use $setIntersection operator in the aggregation pipeline to find out the result.
// Note that your dataset may have duplicate entries for some films. You do not need to count the duplicate entries.
// To get a count after you have defined your pipeline, there are two simple methods.
// add the $count stage to the end of your pipeline
// you will learn about this stage shortly!
// db.movies.aggregate([
//   {$stage1},
//   {$stage2},
//   {...$stageN},
//   { $count: "labors of love" }
// ])
// or use itcount()
// db.movies.aggregate([
//   {$stage1},
//   {$stage2},
//   {...$stageN}
// ]).itcount()
// How many movies are "labors of love"?
db.movies.aggregate([
  {
    $match: {
      cast: { $elemMatch: { $exists: true } },
      directors: { $elemMatch: { $exists: true } },
      writers: { $elemMatch: { $exists: true } },
    },
  },
  {
    $project: {
      _id: 0,
      cast: 1,
      directors: 1,
      writers: {
        $map: {
          input: "$writers",
          as: "writer",
          in: {
            $arrayElemAt: [
              {
                $split: ["$$writer", " ("],
              },
              0,
            ],
          },
        },
      },
    },
  },
  {
    $project: {
      labor_of_love: {
        $gt: [
          { $size: { $setIntersection: ["$cast", "$directors", "$writers"] } },
          0,
        ],
      },
    },
  },
  {
    $match: { labor_of_love: true },
  },
  {
    $count: "labors of love",
  },
]);
// explanation
// 1. match only documents with cast, directors, and writers
// 2. project cast, directors, and writers as array of names
// 3. project labor_of_love as boolean
// 4. match only documents with labor_of_love as true
// 5. count documents

// Problem 5
// MongoDB has another movie night scheduled. This time, we polled employees for their favorite actress or actor, and got these results
// favorites = [
//   "Sandra Bullock",
//   "Tom Hanks",
//   "Julia Roberts",
//   "Kevin Spacey",
//   "George Clooney",
// ];
// For movies released in the USA with a tomatoes.viewer.rating greater than or equal to 3, calculate a new field called num_favs that represets how many favorites appear in the cast field of the movie.
// Sort your results by num_favs, tomatoes.viewer.rating, and title, all in descending order.
// What is the title of the 25th film in the aggregation result?
// My solution
var favorites = [
  "Sandra Bullock",
  "Tom Hanks",
  "Julia Roberts",
  "Kevin Spacey",
  "George Clooney",
];
db.movies
  .aggregate([
    {
      $match: {
        cast: { $elemMatch: { $exists: true } },
        countries: { $elemMatch: { $exists: true } },
        "tomatoes.viewer.rating": { $exists: true },
      },
    },
    {
      $project: {
        _id: 0,
        title: 1,
        cast: 1,
        countries: 1,
        "tomatoes.viewer.rating": 1,
      },
    },
    {
      $match: {
        countries: { $all: ["USA"] },
        "tomatoes.viewer.rating": { $gte: 3 },
      },
    },
    {
      $addFields: {
        num_favs: {
          $size: {
            $setIntersection: ["$cast", favorites],
          },
        },
      },
    },
    {
      $sort: {
        num_favs: -1,
        "tomatoes.viewer.rating": -1,
        title: -1,
      },
    },
    {
      $skip: 24,
    },
    {
      $limit: 1,
    },
  ])
  .pretty();
// explanation
// 1. match only documents with cast, countries, and tomatoes.viewer.rating
// 2. project title, cast, countries, and tomatoes.viewer.rating
// 3. match only documents with countries as USA and tomatoes.viewer.rating as >= 3
// 4. add num_favs as size of set intersection of cast and favorites
// 5. sort by num_favs, tomatoes.viewer.rating, and title
// 6. skip 24 documents
// 7. limit 1 document

// MongoDB University's solution
var favorites = [
  "Sandra Bullock",
  "Tom Hanks",
  "Julia Roberts",
  "Kevin Spacey",
  "George Clooney",
];

db.movies.aggregate([
  {
    $match: {
      "tomatoes.viewer.rating": { $gte: 3 },
      countries: "USA",
      cast: {
        $in: favorites,
      },
    },
  },
  {
    $project: {
      _id: 0,
      title: 1,
      "tomatoes.viewer.rating": 1,
      num_favs: {
        $size: {
          $setIntersection: ["$cast", favorites],
        },
      },
    },
  },
  {
    $sort: { num_favs: -1, "tomatoes.viewer.rating": -1, title: -1 },
  },
  {
    $skip: 24,
  },
  {
    $limit: 1,
  },
]);
// explanation
// 1. match only documents with tomatoes.viewer.rating as >= 3, countries as USA, and cast as favorites
// 2. project title, tomatoes.viewer.rating, and num_favs as size of set intersection of cast and favorites
// 3. sort by num_favs, tomatoes.viewer.rating, and title
// 4. skip 24 documents
// 5. limit 1 document

// Problem 6
// Calculate an average rating for each movie in our collection where English is an available language, the minimum imdb.rating is at least 1, the minimum imdb.votes is at least 1, and it was released in 1990 or after.
// You'll be required to rescale (or normalize) imdb.votes.
// The formula to rescale imdb.votes and calculate normalized_rating is included as a handout.
// What film has the lowest normalized_rating?
db.movies.aggregate([
  {
    $match: {
      year: { $gte: 1990 },
      languages: { $in: ["English"] },
      "imdb.votes": { $gte: 1 },
      "imdb.rating": { $gte: 1 },
    },
  },
  {
    $project: {
      _id: 0,
      title: 1,
      "imdb.rating": 1,
      "imdb.votes": 1,
      normalized_rating: {
        $avg: [
          "$imdb.rating",
          {
            $add: [
              1,
              {
                $multiply: [
                  9,
                  {
                    $divide: [
                      { $subtract: ["$imdb.votes", 5] },
                      { $subtract: [1521105, 5] },
                    ],
                  },
                ],
              },
            ],
          },
        ],
      },
    },
  },
  { $sort: { normalized_rating: 1 } },
  { $limit: 1 },
]);
// explanation
// 1. match only documents with year as >= 1990, languages as English, imdb.votes as >= 1, and imdb.rating as >= 1
// 2. project title, imdb.rating, imdb.votes, and normalized_rating as average of imdb.rating and rescaled imdb.votes
// 3. sort by normalized_rating
// 4. limit 1 document

// Problem 7
// In the last lab, we calculated a normalized rating that required us to know what the minimum and maximum values for imdb.votes were.
// These values were found using the $group stage!
// For all films that won at least 1 Oscar, calculate the standard deviation, highest, lowest, and average imdb.rating.
// Use the sample standard deviation expression.
// HINT - All movies in the collection that won an Oscar begin with a string resembling one of the following in their awards field
// Won 13 Oscars
// Won 1 Oscar
// Select the correct answer from the choices below. Numbers are truncated to 4 decimal places.
db.movies.aggregate([
  {
    $match: {
      awards: /Won \d{1,2} Oscars?/,
    },
  },
  {
    $group: {
      _id: null,
      highest_rating: { $max: "$imdb.rating" },
      lowest_rating: { $min: "$imdb.rating" },
      average_rating: { $avg: "$imdb.rating" },
      deviation: { $stdDevSamp: "$imdb.rating" },
    },
  },
]);
// explanation
// 1. match only documents with awards as Won <number> Oscars?
// 2. group by null and calculate highest_rating as max of imdb.rating, lowest_rating as min of imdb.rating, average_rating as avg of imdb.rating, and deviation as sample standard deviation of imdb.rating

// Problem 8
// Let's use our increasing knowledge of the Aggregation Framework to explore our movies collection in more detail.
// We'd like to calculate how many movies every cast member has been in and get an average imdb.rating for each cast member.
// What is the name, number of movies, and average rating (truncated to one decimal)
// for the cast member that has been in the most number of movies with English as an available language?
// Provide the input in the following order and format
// { "_id": "First Last", "numFilms": 1, "average": 1.1 }
// my solution
db.movies.aggregate([
  {
    $match: {
      languages: { $in: ["English"] },
    },
  },
  {
    $unwind: "$cast",
  },
  {
    $group: {
      _id: "$cast",
      num_movies: { $sum: 1 },
      avg_imdb: { $avg: "$imdb.rating" },
    },
  },
  {
    $sort: { num_movies: -1, _id: -1 },
  },
  {
    $limit: 1,
  },
]);
// explanation
// 1. match only documents with languages as English
// 2. unwind cast
// 3. group by cast and calculate num_movies as sum of 1 and avg_imdb as avg of imdb.rating
// 4. sort by num_movies and _id
// 5. limit 1 document

// MongoDB University's solution
db.movies.aggregate([
  {
    $match: {
      languages: "English",
    },
  },
  {
    $project: { _id: 0, cast: 1, "imdb.rating": 1 },
  },
  {
    $unwind: "$cast",
  },
  {
    $group: {
      _id: "$cast",
      numFilms: { $sum: 1 },
      average: { $avg: "$imdb.rating" },
    },
  },
  {
    $project: {
      numFilms: 1,
      average: {
        $divide: [{ $trunc: { $multiply: ["$average", 10] } }, 10],
      },
    },
  },
  {
    $sort: { numFilms: -1 },
  },
  {
    $limit: 1,
  },
]);
// explanation
// 1. match only documents with languages as English
// 2. project cast and imdb.rating
// 3. unwind cast
// 4. group by cast and calculate numFilms as sum of 1 and average as avg of imdb.rating
// 5. project numFilms and average as truncated to 1 decimal of avg of imdb.rating
// 6. sort by numFilms
// 7. limit 1 document
// $trunc is a new operator in MongoDB 4.2 that truncates a number to an integer

// Problem 9
// Which alliance from air_alliances flies the most routes with either a Boeing 747
// or an Airbus A380 (abbreviated 747 and 380 in air_routes)?
db.air_routes.aggregate([
  {
    $match: {
      airplane: /747|380/,
    },
  },
  {
    $lookup: {
      from: "air_alliances",
      foreignField: "airlines",
      localField: "airline.name",
      as: "alliance",
    },
  },
  {
    $unwind: "$alliance",
  },
  {
    $group: {
      _id: "$alliance.name",
      count: { $sum: 1 },
    },
  },
  {
    $sort: { count: -1 },
  },
]);
// explanation
// 1. match only documents with airplane as 747 or 380
// 2. lookup air_alliances and match airlines with airline.name and store in alliance
// 3. unwind alliance
// 4. group by alliance.name and calculate count as sum of 1
// 5. sort by count

// sample alliance document
// {
//   "_id" : ObjectId("5980bef9a39d0ba3c650ae9d"),
//   "name" : "OneWorld",
//   "airlines" : [
//           "Air Berlin",
//           "American Airlines",
//           "British Airways",
//           "Cathay Pacific",
//           "Finnair",
//           "Iberia Airlines",
//           "Japan Airlines",
//           "LATAM Chile",
//           "LATAM Brasil",
//           "Malaysia Airlines",
//           "Canadian Airlines",
//           "Qantas",
//           "Qatar Airways",
//           "Royal Jordanian",
//           "SriLankan Airlines",
//           "S7 Airlines"
//   ]
// }
// sample air_route document
// {
//   "_id" : ObjectId("56e9b39b732b6122f877fa31"),
//   "airline" : {
//           "id" : 410,
//           "name" : "Aerocondor",
//           "alias" : "2B",
//           "iata" : "ARD"
//   },
//   "src_airport" : "CEK",
//   "dst_airport" : "KZN",
//   "codeshare" : "",
//   "stops" : 0,
//   "airplane" : "CR2"
// }

// Problem 10
// Now that you have been introduced to $graphLookup,
// let's use it to solve an interesting need.
// You are working for a travel agency and would like to find routes for a client!
// For this exercise, we'll be using the air_airlines, air_alliances,
// and air_routes collections in the aggregations database.
// The air_airlines collection will use the following schema:
// {
//   "_id" : ObjectId("56e9b497732b6122f8790280"),
//   "airline" : 4,
//   "name" : "2 Sqn No 1 Elementary Flying Training School",
//   "alias" : "",
//   "iata" : "WYT",
//   "icao" : "",
//   "active" : "N",
//   "country" : "United Kingdom",
//   "base" : "HGH"
// }
// The air_routes collection will use this schema:
// {
//   "_id" : ObjectId("56e9b39b732b6122f877fa31"),
//   "airline" : {
//           "id" : 410,
//           "name" : "Aerocondor",
//           "alias" : "2B",
//           "iata" : "ARD"
//   },
//   "src_airport" : "CEK",
//   "dst_airport" : "KZN",
//   "codeshare" : "",
//   "stops" : 0,
//   "airplane" : "CR2"
// }
// Finally, the air_alliances collection will show the airlines that are in each alliance,
// with this schema:
// {
//   "_id" : ObjectId("581288b9f374076da2e36fe5"),
//   "name" : "Star Alliance",
//   "airlines" : [
//           "Air Canada",
//           "Adria Airways",
//           "Avianca",
//           "Scandinavian Airlines",
//           "All Nippon Airways",
//           "Brussels Airlines",
//           "Shenzhen Airlines",
//           "Air China",
//           "Air New Zealand",
//           "Asiana Airlines",
//           "Copa Airlines",
//           "Croatia Airlines",
//           "EgyptAir",
//           "TAP Portugal",
//           "United Airlines",
//           "Turkish Airlines",
//           "Swiss International Air Lines",
//           "Lufthansa",
//           "EVA Air",
//           "South African Airways",
//           "Singapore Airlines"
//   ]
// }
// Determine the approach that satisfies the following question in the most efficient manner:
// Find the list of all possible distinct destinations,
// with at most one layover, departing from the base airports of airlines from Germany,
// Spain or Canada that are part of the "OneWorld" alliance.
// Include both the destination and which airline services that location.
// As a small hint, you should find 158 destinations.
// Select the correct pipeline from the following set of options:
db.air_alliances.aggregate([
  {
    $match: { name: "OneWorld" },
  },
  {
    $graphLookup: {
      startWith: "$airlines",
      from: "air_airlines",
      connectFromField: "name",
      connectToField: "name",
      as: "airlines",
      maxDepth: 0,
      restrictSearchWithMatch: {
        country: { $in: ["Germany", "Spain", "Canada"] },
      },
    },
  },
  {
    $graphLookup: {
      startWith: "$airlines.base",
      from: "air_routes",
      connectFromField: "dst_airport",
      connectToField: "src_airport",
      as: "connections",
      maxDepth: 1,
    },
  },
  {
    $project: {
      validAirlines: "$airlines.name",
      "connections.dst_airport": 1,
      "connections.airline.name": 1,
    },
  },
  { $unwind: "$connections" },
  {
    $project: {
      isValid: {
        $in: ["$connections.airline.name", "$validAirlines"],
      },
      "connections.dst_airport": 1,
    },
  },
  { $match: { isValid: true } },
  {
    $group: {
      _id: "$connections.dst_airport",
    },
  },
]);
// explanation
// 1. match only documents with alliance name as OneWorld
// 2. graphLookup to find airlines from Germany, Spain or Canada
// 3. graphLookup to find routes from base airports of airlines from Germany, Spain or Canada
// 4. project valid airlines and destination airport
// 5. unwind connections
// 6. project isValid and destination airport
// 7. match only isValid
// 8. group by destination airport

// Problem 11
// Lab - $facets
// How many movies are in both the top ten highest rated movies according to the imdb.rating
// and the metacritic fields?
// We should get these results with exactly one access to the database.
// Hint: What is the intersection?
db.movies.aggregate([
  {
    $match: {
      metacritic: { $gte: 0 },
      "imdb.rating": { $gte: 0 },
    },
  },
  {
    $project: {
      _id: 0,
      metacritic: 1,
      imdb: 1,
      title: 1,
    },
  },
  {
    $facet: {
      top_metacritic: [
        {
          $sort: {
            metacritic: -1,
            title: 1,
          },
        },
        {
          $limit: 10,
        },
        {
          $project: {
            title: 1,
          },
        },
      ],
      top_imdb: [
        {
          $sort: {
            "imdb.rating": -1,
            title: 1,
          },
        },
        {
          $limit: 10,
        },
        {
          $project: {
            title: 1,
          },
        },
      ],
    },
  },
  {
    $project: {
      movies_in_both: {
        $setIntersection: ["$top_metacritic", "$top_imdb"],
      },
    },
  },
]);
// explanation
// 1. match only documents with metacritic and imdb rating greater than or equal to 0
// 2. project metacritic, imdb rating, and title
// 3. facet top metacritic and top imdb
// 4. sort top metacritic by metacritic and title
// 5. limit top metacritic to 10
// 6. project title
// 7. sort top imdb by imdb rating and title
// 8. limit top imdb to 10
// 9. project title
// 10. project movies_in_both by setIntersection of top_metacritic and top_imdb
