package io.confluent.demo;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.StringTokenizer;
import java.util.stream.Collectors;

public class Parser {

    static List<CharSequence> parseArray(String text) {
        return Collections.list(new StringTokenizer(text, "|")).stream()
                .map(token -> (String) token)
                .collect(Collectors.toList());
    }

    //id::title::release_date::?::country::rating::?::genres::actors::director::composer::screenwriter::production_companies
    static Movie parseMovie(String text) {
        StringTokenizer st = new StringTokenizer(text, "::");
        String id = st.nextToken();
        String title = st.nextToken();
        String releaseDate = st.nextToken();
        st.nextToken(); //skip
        String country = st.nextToken();
        String rating = st.nextToken();
        st.nextToken(); // skip
        String genres = st.nextToken();
        String actors = st.nextToken();
        String directors = st.nextToken();
        String composers = st.nextToken();
        String screenwriters = st.nextToken();
        String cinematographer = st.nextToken();
        String productionCompanies = st.nextToken();

        Movie movie = new Movie();
        movie.setMovieId(Integer.parseInt(id));
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
        StringTokenizer st = new StringTokenizer(text, "::");

        st.nextToken(); // skip user ID, nobody even cares about you
        String movieId = st.nextToken();
        String userRating = st.nextToken();

        Rating rating = new Rating();
        rating.setMovieId(Integer.parseInt(movieId));
        rating.setRating(Float.parseFloat(userRating));

        return rating;
    }

}
