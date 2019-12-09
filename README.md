# Big Data Final Project

### Thomas Lauerman

# Introduction

Movies. The plays of the digital age. You watch them, I watch them. You can watch them in 3D, you can even
watch them in 4D, allegedly. Regardless of how you watch them, they exist! That means there's probably data about them,
and in this case that holds to be true. Some larger data, in fact. Some might say Big Data.

# Why this dataset?

So, why movies? Well as previously discussed, I watch them, and I find them to be quite enjoyable.
So you can imagine that when I found out there was an entire dataset dedicated to them I was here for it.
Some specific features of the dataset did stand out to me though:

- Basic movie metadata: title, release date, genre
- Poster for each movie
- User ratings for each movie

These were the features that I decided to focus on for my questions.

# Analyzing Movie Posters

I knew from the beginning that I wanted to do something special with the movie posters.
I settled for doing something mildly interesting with movie posters.

Before I started doing any machine learning though, I knew I had to break down the poster images
into data that was a little more manageable. Using Python scripts I calculated the average
hue, saturation and value of all the pixels in each of the images to keep the complexity low.

Some of you may be immediately skeptical of this method, and rightfully so. We'll see in a moment how this affected
my plans for machine learning. However, before any machines could do their learning I first had to
graph my newly preprocessed data.

![HSV Distribution in Movie Posters](/img/hsv_dist.png)

###### Note that the size of the dot indicates the relative saturation of that color

This graph shows some interesting features:

- Red and orange are incredibly popular movie poster color choices
- Blue is fairly popular, though less than red
- Greens and magentas are notably underused

Since this dataset has a time component, the release date, I decided to make a plot that was more chronological as well.

![Average Hue Distribution in Movie Posters over Time](/img/hue_over_time.png)

This graph has some notable trends as well:

- There were markedly few movies coming out in 1880
- Red has always been a popular color choice for movie posters
- Blues have only become popular in the past 60 years or so
- Greens and magentas are used now more than ever (probably due to there being more movies now more than ever)

# Question 1

So with all this (big) data, what kind of questions can I answer? Well, the one I decided on is as follows:

How well can I classify the genre of a movie based solely on it's poster?

Let's find out.

# Immediately I ran into issues

The thing about movies is that they typically have multiple genres.

One important aspect of classification is that it assumes there to be one correct answer for every prediction.

So how to I create a model that accounts for multiple correct answers?

# Change the data

Although Dr. Lewis was very helpful providing me a clever potential solution involving regression, multi-hot-encoding and more I decided in the end to keep it simple.

If I could find some way to pick one genre from the many that most movies have, I could simplify the problem greatly. It might also be nice to reduce the total 20 possible genres to a number that's more manageable.

Solution: Pick the 5 most common genres. Keep only the movies that have at least one of those genres. In the event that one movie is classified as more than one of those common genres, keep the least common genre.

# Back to classification!

Now that the problem is in the format of any trivial classification problem, the rest is simple. I decided to go with a Random Forest Classifier with ten trees, the outline of my model is as follows:

- Random Forest Classifier
  - 10 Trees
  - Inputs: average hue, average saturation, average value
  - Ouptut: one of 5 popular movie genres
    - Drama, Comedy, Thriller, Romance, or Action

# Results

Remember when I said that some healthy skepticism with regards to averaging hue, saturation and value over an entire image was well warranted? Here's why.

The classifier's accuracy came out to about 29.17%.

While pretty abysmal, it's interesting to see that the accuracy got much of anything right at all given the terrible input I'm giving it.

# What went wrong

I only realized this in retrospect, but averaging the HSV values in an image loses a lot of information, and what results is a mostly same-y mess.

I was really trying to avoid over-complicating the problem, but I think that in doing so I dumbed it down so much that it was hardly doable anymore. I never looked into how well this is supported by Spark, but I think it would be interesting to go at my original question using something like a Convolutional Neural Network to pick out non-obvious features of movie posters and hopefully better classify genres.

# Let's move on

Sad, tired, and discouraged I poured over my dataset to try to find something else to analyze.
When I stumbled upon the ratings section of my dataset I immediately thought of a reccomendation system.
Before we do that though, let's dig into the dataset a little more.

![Frequency of Movie Ratings](/img/freq_movie_ratings.png)

This graph displays the number of movies rated at 0.5 stars, 1 star, 1.5 stars, 2 stars, and so on to 5 stars.
It seems that most people rate movies in the middle of the road with a 3 or a 4-star rating. Meanwhile, 0.5 and 1.5 are the least common ratings.

# Question 2

If the goal is to build a reccomendation system then the logical question is as follows:

Can we build an intelligent movie reccomendation system based on the reviews of other users?

Thankfully Spark has a great reccomendation algorithm built-in, so I'll be using the Alternating Least Squares (ALS) algorithm they provide to build my reccomendations. To summarize:

- ALS Model
  - Max iterations: 5
  - Regularization Parameter: 0.1
  - Inputs: (movieId, rating) pairs
  - Ouptut: Movie reccomendations

# Results

With a root mean squared error of about 0.844 (in a range from 0.0-5.0) I'd say this model was more successful that the genre classifier. It still leaves a lot to be desired in terms of accuracy, but I'm not sure if this is my fault due to some oversight or if this is just a limitation of ALS.
