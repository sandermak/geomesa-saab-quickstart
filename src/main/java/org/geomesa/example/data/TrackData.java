/*
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

package org.geomesa.example.data;

import org.geotools.filter.text.cql2.CQLException;
import org.geotools.filter.text.ecql.ECQL;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.geotools.data.Query;
import org.geotools.factory.Hints;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.locationtech.geomesa.utils.interop.SimpleFeatureTypes;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;

import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class SaabTrackData {

    private SimpleFeatureType sft = null;
    private List<SimpleFeature> features = null;
    private List<Query> queries = null;
    private Filter subsetFilter = null;

    public String getTypeName() {
        return "saab-quickstart";
    }

    public SimpleFeatureType getSimpleFeatureType() {
        if (sft == null) {
            StringBuilder attributes = new StringBuilder();
            attributes.append("trackid:String:index=true,");
            attributes.append("callsign:String,");
            attributes.append("updatetime:Date,");
            attributes.append("*loc:Point:srid=4326"); // the "*" denotes the default geometry (used for indexing)

            // create the simple-feature type - use the GeoMesa 'SimpleFeatureTypes' class for best compatibility
            // may also use geotools DataUtilities or SimpleFeatureTypeBuilder, but some features may not work
            sft = SimpleFeatureTypes.createType(getTypeName(), attributes.toString());

            // use the user-data (hints) to specify which date field to use for primary indexing
            // if not specified, the first date attribute (if any) will be used
            // could also use ':default=true' in the attribute specification string
            sft.getUserData().put(SimpleFeatureTypes.DEFAULT_DATE_KEY, "updatetime");
        }
        return sft;
    }

    public List<SimpleFeature> getTestData() {
        if (features == null) {
            List<SimpleFeature> features = new ArrayList<>();

            // read the saab CSV
            URL input = getClass().getClassLoader().getResource("trackdata.csv");
            if (input == null) {
                throw new RuntimeException("Couldn't load resource trackdata.csv");
            }

            // date parser corresponding to the CSV format
            DateTimeFormatter dateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS", Locale.US);

            // use a geotools SimpleFeatureBuilder to create our features
            SimpleFeatureBuilder builder = new SimpleFeatureBuilder(getSimpleFeatureType());

            try (CSVParser parser = CSVParser.parse(input, StandardCharsets.UTF_8, CSVFormat.DEFAULT)) {
                System.out.println("Reading records from " + input.toString());
                for (CSVRecord record : parser) {
                    // use apache commons-csv to parse the t-drive file
                    // pull out the fields corresponding to our simple feature attributes
                    builder.set("trackid", record.get(0));
                    builder.set("callsign", record.get(1));

                    // some dates are converted implicitly, so we can set them as strings
                    // however, the date format here isn't one that is converted, so we parse it into a java.util.Date
                    builder.set("updatetime", Date.from(LocalDateTime.parse(record.get(2), dateFormat).toInstant(ZoneOffset.UTC)));

                    // we can use WKT (well-known-text) to represent geometries
                    // note that we use longitude first ordering
                    double longitude = Double.parseDouble(record.get(3));
                    double latitude = Double.parseDouble(record.get(4));
                    builder.set("loc", "POINT (" + longitude + " " + latitude + ")");

                    // be sure to tell GeoTools explicitly that we want to use the ID we provided
                     builder.featureUserData(Hints.USE_PROVIDED_FID, Boolean.TRUE);

                    // build the feature - this also resets the feature builder for the next entry
                    // use the taxi ID as the feature ID
                    features.add(builder.buildFeature(record.get(0)));
                }
            } catch (Throwable e) {
                throw new RuntimeException("Error reading Saab data:", e);
            }
            this.features = Collections.unmodifiableList(features);
        }
        return features;
    }

    public List<Query> getTestQueries() {
        if (queries == null) {
            List<Query> queries = new ArrayList<>();
            // this data set is meant to show streaming updates over time, so just return all features
            queries.add(new Query(getTypeName(), Filter.INCLUDE));
            this.queries = Collections.unmodifiableList(queries);
        }
        return queries;
    }

    public Filter getSubsetFilter() {
        return Filter.INCLUDE;
    }

    /**
     * Creates a geotools filter based on a bounding box and date range
     *
     * @param geomField geometry attribute name
     * @param x0 bounding box min x value
     * @param y0 bounding box min y value
     * @param x1 bounding box max x value
     * @param y1 bounding box max y value
     * @param dateField date attribute name
     * @param t0 minimum time, exclusive, in the format "yyyy-MM-dd'T'HH:mm:ss.SSSZ"
     * @param t1 maximum time, exclusive, in the format "yyyy-MM-dd'T'HH:mm:ss.SSSZ"
     * @param attributesQuery any additional query string, or null
     * @return filter object
     * @throws CQLException if invalid CQL
     */
    static Filter createFilter(String geomField, double x0, double y0, double x1, double y1,
                               String dateField, String t0, String t1,
                               String attributesQuery) throws CQLException {

        // there are many different geometric predicates that might be used;
        // here, we just use a bounding-box (BBOX) predicate as an example.
        // this is useful for a rectangular query area
        String cqlGeometry = "BBOX(" + geomField + ", " + x0 + ", " + y0 + ", " + x1 + ", " + y1 + ")";

        // there are also quite a few temporal predicates; here, we use a
        // "DURING" predicate, because we have a fixed range of times that
        // we want to query
        String cqlDates = "(" + dateField + " DURING " + t0 + "/" + t1 + ")";

        // there are quite a few predicates that can operate on other attribute
        // types; the GeoTools Filter constant "INCLUDE" is a default that means
        // to accept everything
        String cqlAttributes = attributesQuery == null ? "INCLUDE" : attributesQuery;

        String cql = cqlGeometry + " AND " + cqlDates  + " AND " + cqlAttributes;

        // we use geotools ECQL class to parse a CQL string into a Filter object
        return ECQL.toFilter(cql);
    }
}
