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
