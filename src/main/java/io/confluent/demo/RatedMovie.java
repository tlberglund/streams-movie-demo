package io.confluent.demo;

public class RatedMovie {
   private String title;
   private int releaseYear;
   private float rating;

   @Override
   public String toString() {
      return title + "(" + releaseYear + "): " + rating;
   }

   public String getTitle() {
      return title;
   }

   public int getReleaseYear() {
      return releaseYear;
   }

   public float getRating() {
      return rating;
   }

   public RatedMovie(String title, int releaseYear, float rating) {
      this.title = title;
      this.releaseYear = releaseYear;
      this.rating = rating;
   }
}
