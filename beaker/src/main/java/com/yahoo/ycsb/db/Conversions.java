package com.yahoo.ycsb.db;

import javax.annotation.Nullable;

import scala.collection.JavaConverters;
import scala.collection.immutable.Map$;

/**
 * A Java-Scala adapter.
 */
public final class Conversions {

  private Conversions() {}

  /**
   * Convert Java value to Scala option.
   *
   * @param x Java value.
   * @return Scala option.
   */
  public static <A> scala.Option<A> toOption(@Nullable A x) {
    return scala.Option.<A>apply(x);
  }

  /**
   * Convert Scala option to Java value.
   *
   * @param x Scala option.
   * @return Java value.
   */
  @Nullable
  public static <A> A toOption(scala.Option<A> x) {
    return (x.isDefined()) ? x.get() : null;
  }

  /**
   * Convert Java map to Scala immutable map.
   *
   * @param map Java map.
   * @return Scala map.
   */
  @SuppressWarnings("unchecked")
  public static <A, B> scala.collection.immutable.Map<A, B> toMap(java.util.Map<A, B> map) {
    return (scala.collection.immutable.Map<A, B>) Map$.MODULE$.apply(
        JavaConverters.mapAsScalaMap(map).toSeq()
    );
  }

  /**
   * Convert Scala immutable map to Java map.
   *
   * @param map Scala map.
   * @return Java map.
   */
  public static <A, B> java.util.Map<A, B> toMap(scala.collection.immutable.Map<A, B> map) {
    return JavaConverters.mapAsJavaMap(map);
  }

  /**
   * Convert Java set to Scala immutable set.
   *
   * @param set Java set.
   * @return Scala set.
   */
  public static <A> scala.collection.immutable.Set<A> toSet(java.util.Set<A> set) {
    return JavaConverters.asScalaSet(set).toSet();
  }

  /**
   * Convert Scala immutable set to Java set.
   *
   * @param set Scala set.
   * @return Java set.
   */
  public static <A> java.util.Set<A> toSet(scala.collection.immutable.Set<A> set) {
    return JavaConverters.setAsJavaSet(set);
  }

  /**
   * Convert Java list to Scala immutable list.
   *
   * @param list Java list.
   * @return Scala list.
   */
  public static <A> scala.collection.immutable.List<A> toList(java.util.List<A> list) {
    return JavaConverters.asScalaBuffer(list).<A>toList();
  }

  /**
   * Convert a Scala immutable seq to a Java list.
   *
   * @param seq Scala seq.
   * @return Java list.
   */
  public static <A> java.util.List<A> toList(scala.collection.immutable.Seq<A> seq) {
    return JavaConverters.seqAsJavaList(seq);
  }

}