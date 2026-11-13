package dev.kostas.trackstore.track;

import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import jakarta.validation.ValidatorFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Set;
import java.util.stream.Collectors;

import static dev.kostas.trackstore.track.TrackUpdateFixtures.aTrackUpdate;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Plain unit tests for the Bean Validation rules on {@link TrackUpdate}.
 * No Spring context is started: the validator is built directly from Hibernate Validator.
 */
class TrackUpdateValidationTest {

    private static ValidatorFactory validatorFactory;
    private static Validator validator;

    @BeforeAll
    static void createValidator() {
        validatorFactory = Validation.buildDefaultValidatorFactory();
        validator = validatorFactory.getValidator();
    }

    @AfterAll
    static void closeValidator() {
        validatorFactory.close();
    }

    /** Names of the fields that failed validation, e.g. {@code ["latitude"]}. */
    private static Set<String> invalidFields(TrackUpdate update) {
        return validator.validate(update).stream()
                .map(ConstraintViolation::getPropertyPath)
                .map(Object::toString)
                .collect(Collectors.toSet());
    }

    @Test
    void fullyPopulatedUpdateIsValid() {
        assertThat(invalidFields(aTrackUpdate().build())).isEmpty();
    }

    @Test
    void optionalFieldsMayBeMissing() {
        TrackUpdate update = aTrackUpdate().altitude(null).heading(null).build();

        assertThat(invalidFields(update)).isEmpty();
    }

    @Nested
    class TrackId {

        @ParameterizedTest
        @NullAndEmptySource
        @ValueSource(strings = {" ", "\t"})
        void mustNotBeBlank(String trackId) {
            assertThat(invalidFields(aTrackUpdate().trackId(trackId).build()))
                    .containsExactly("trackId");
        }
    }

    @Nested
    class Timestamp {

        @Test
        void isRequired() {
            assertThat(invalidFields(aTrackUpdate().timestamp(null).build()))
                    .containsExactly("timestamp");
        }
    }

    @Nested
    class Latitude {

        @Test
        void isRequired() {
            assertThat(invalidFields(aTrackUpdate().latitude(null).build()))
                    .containsExactly("latitude");
        }

        @ParameterizedTest
        @ValueSource(doubles = {-90.0, 0.0, 90.0})
        void acceptsValuesWithinRangeIncludingBounds(double latitude) {
            assertThat(invalidFields(aTrackUpdate().latitude(latitude).build())).isEmpty();
        }

        @ParameterizedTest
        @ValueSource(doubles = {-90.0001, 90.0001, -180.0, 180.0})
        void rejectsValuesOutsideRange(double latitude) {
            assertThat(invalidFields(aTrackUpdate().latitude(latitude).build()))
                    .containsExactly("latitude");
        }
    }

    @Nested
    class Longitude {

        @Test
        void isRequired() {
            assertThat(invalidFields(aTrackUpdate().longitude(null).build()))
                    .containsExactly("longitude");
        }

        @ParameterizedTest
        @ValueSource(doubles = {-180.0, 0.0, 180.0})
        void acceptsValuesWithinRangeIncludingBounds(double longitude) {
            assertThat(invalidFields(aTrackUpdate().longitude(longitude).build())).isEmpty();
        }

        @ParameterizedTest
        @ValueSource(doubles = {-180.0001, 180.0001, 360.0})
        void rejectsValuesOutsideRange(double longitude) {
            assertThat(invalidFields(aTrackUpdate().longitude(longitude).build()))
                    .containsExactly("longitude");
        }
    }

    @Nested
    class Altitude {

        @ParameterizedTest
        @ValueSource(doubles = {0.0, 0.5, 20_000.0})
        void acceptsZeroAndAbove(double altitude) {
            assertThat(invalidFields(aTrackUpdate().altitude(altitude).build())).isEmpty();
        }

        @ParameterizedTest
        @ValueSource(doubles = {-0.0001, -100.0})
        void rejectsNegativeValues(double altitude) {
            assertThat(invalidFields(aTrackUpdate().altitude(altitude).build()))
                    .containsExactly("altitude");
        }
    }

    @Nested
    class Heading {

        @ParameterizedTest
        @ValueSource(doubles = {0.0, 180.0, 359.9999})
        void acceptsZeroUpToButExcluding360(double heading) {
            assertThat(invalidFields(aTrackUpdate().heading(heading).build())).isEmpty();
        }

        @ParameterizedTest
        @ValueSource(doubles = {-0.0001, 360.0, 400.0})
        void rejectsValuesOutsideRange(double heading) {
            assertThat(invalidFields(aTrackUpdate().heading(heading).build()))
                    .containsExactly("heading");
        }
    }

    @Nested
    class IdentityField {

        @Test
        void isRequired() {
            assertThat(invalidFields(aTrackUpdate().identity(null).build()))
                    .containsExactly("identity");
        }

    }

    @Test
    void reportsEveryInvalidFieldAtOnce() {
        TrackUpdate update = new TrackUpdate("", null, 91.0, null, -1.0, 360.0, null);

        assertThat(invalidFields(update)).containsExactlyInAnyOrder(
                "trackId", "timestamp", "latitude", "longitude", "altitude", "heading", "identity");
    }
}
