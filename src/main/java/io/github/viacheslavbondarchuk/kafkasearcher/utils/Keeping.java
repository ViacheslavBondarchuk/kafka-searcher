package io.github.viacheslavbondarchuk.kafkasearcher.utils;

import java.util.Objects;
import java.util.function.BiPredicate;
import java.util.function.BinaryOperator;

/**
 * author: viacheslavbondarchuk
 * date: 7/20/2023
 * time: 12:51 PM
 **/
public final class Keeping {
    private Keeping() {
    }

    public static <A> A keepLeft(A left, A right) {
        return left;
    }

    public static <A> A keepRight(A left, A right) {
        return right;
    }

    public static <A> A keepRightDifferent(A left, A right) {
        return Objects.equals(left, right) ? null : right;
    }

    public static <A> A keepLeftDifferent(A left, A right) {
        return Objects.equals(left, right) ? null : left;
    }

    public static <A> BinaryOperator<A> keepByCondition(BiPredicate<A, A> predicate) {
        return (v1, v2) -> predicate.test(v1, v2) ? v1 : v2;
    }
}
