package io.confluent.demo;

import java.util.Collections;
import java.util.List;
import java.util.StringTokenizer;
import java.util.stream.Collectors;
import javax.json.*;

public class Parser {

    static List<CharSequence> parseArray(String text) {
        return Collections.list(new StringTokenizer(text, "|")).stream()
                .map(token -> (String) token)
                .collect(Collectors.toList());
    }

   static Movie parseMovie(String text) {
      String[] tokens = text.split("\\:\\:");
      String id = tokens[0];
      String title = tokens[1];
      String releaseDate = tokens[2];
      String country = tokens[4];
      String rating = tokens[5];
      String genres = tokens[7];
      String actors = tokens[8];
      String directors = tokens[9];
      String composers = tokens[10];
      String screenwriters = tokens[11];
      String cinematographer = tokens[12];
      String productionCompanies = "";
      if(tokens.length > 13) {
         productionCompanies = tokens[13];
      }


      Movie movie = new Movie();
      movie.setMovieId(Long.parseLong(id));
      movie.setTitle(title);
      movie.setReleaseDate(Integer.parseInt(releaseDate));
      movie.setCountry(country);
      movie.setRating(Float.parseFloat(rating));
      movie.setGenres(Parser.parseArray(genres));
      movie.setActors(Parser.parseArray(actors));
      movie.setDirectors(Parser.parseArray(directors));
      movie.setComposers(Parser.parseArray(composers));
      movie.setScreenwriters(Parser.parseArray(screenwriters));
      movie.setCinematographer(cinematographer);
      movie.setProductionCompanies(Parser.parseArray(productionCompanies));

      return movie;
   }


   // userid::movieid::rating
   static Rating parseRating(String text) {
      String[] tokens = text.split("\\:\\:");

      String movieId = tokens[1];
      String userRating = tokens[2];

      Rating rating = new Rating();
      rating.setMovieId(Long.parseLong(movieId));
      rating.setRating(Float.parseFloat(userRating));

      return rating;
   }


   static JsonObject toJson(Rating rating) {
      JsonBuilderFactory factory = Json.createBuilderFactory(null);
      return factory.createObjectBuilder()
         .add("movie_id", rating.getMovieId())
         .add("rating", rating.getRating())
         .build();
   }


   static JsonObject toJson(Movie rating) {
      JsonBuilderFactory factory = Json.createBuilderFactory(null);
      return factory.createObjectBuilder()
              .add("movie_id", rating.getMovieId())
              .add("title", rating.getTitle().toString())
              .add("release_year", rating.getReleaseDate())
              .add("country",rating.getCountry().toString())
              .add("rating", rating.getRating())
              .add("genres", toJsonArrayOfStrings(rating.getGenres()))
              .add("actors", toJsonArrayOfStrings(rating.getActors()))
              .add("directors", toJsonArrayOfStrings(rating.getDirectors()))
              .add("composers", toJsonArrayOfStrings(rating.getComposers()))
              .add("screenwriters", toJsonArrayOfStrings(rating.getScreenwriters()))
              .add("production_companies", toJsonArrayOfStrings(rating.getProductionCompanies()))
              .add("cinematographer", rating.getCinematographer().toString())
              .build();
   }

   static JsonArray toJsonArrayOfStrings(List<CharSequence> elements) {
       JsonArrayBuilder arrayBuilder = Json.createBuilderFactory(null).createArrayBuilder();
       for(CharSequence element : elements) {
          arrayBuilder.add(element.toString());
       }
       return arrayBuilder.build();
   }

}
