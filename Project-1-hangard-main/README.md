# CS460 - Systems for data managment and data science project
## Overview

This initiative engages with multiple programming paradigms prevalent in contemporary data processing architectures at scale. Our dataset is inspired by the MovieLens datasets and comprises the following elements:

* A collection of video content, such as films and TV series, alongside their corresponding genres.
* A record of user interactions, specifically their movie rating activities.

The project is integral to a video streaming platform that necessitates extensive data processing. While our focus does not include the user interface of the application, we contribute significantly to the data served to users. Each title on the platform provides:

* A list of movies deemed similar based on keywords.
* An average user rating.

Behind the scenes, the application triggers Spark jobs that pre-calculate the information served to users. Our primary responsibility within this project is to develop the necessary Spark code that enables this functionality.

## Code documentation
This project is thoroughly documented within the codebase itself. Each function is preceded by clear, detailed comments that explain its purpose, its input parameters, and its output. Furthermore, within each function, numerous line comments explain the role of each variable and the steps taken by the function. This extensive inline documentation makes the project easy to understand and modify.

## Test Results

The codebase has been tested locally and on gitlab, successfully passing all 41 project tests within the expected time frame. 