package org.polleyg;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFnTester;
import static org.hamcrest.Matchers.anyOf;

import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.Matchers.*;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.hamcrest.core.IsNot.not;


//public class TemplatePipelineTest {
//
//    private static DoFnTester<String, TableRow> fnTester;
//
//    @BeforeClass
//    public static void init() {
//        fnTester = DoFnTester.of(new TemplatePipeline.WikiParDo());
//    }
//
//    @AfterClass
//    public static void destroy() {
//        fnTester = null;
//    }
//
//    @Test
//    public void test_parse_CSV_format_successfully_with_tablerow() throws Exception {
//
//        List<String> input = new ArrayList<>();
//
//        input.add("2018,8,13,Wikinews,English,Spanish football: Sevilla signs Aleix Vidal from FC Barcelona,12331,12331,12331");
//        System.out.println("input tab");
//        System.out.println(input);
//        DoFnTester<String, TableRow> tester = DoFnTester.of(new TemplatePipeline.WikiParDo());
//        System.out.println(tester);
//        List<TableRow> output = tester.processBundle(input);
//        System.out.println(output);
//
//        Assert.assertThat(output, is(not(empty())));
//
//        Assert.assertThat(output.get(0).get("number"), is(equalTo("2018")));
//        Assert.assertThat(output.get(0).get("customer_id"), is(equalTo("8")));
//        Assert.assertThat(output.get(0).get("street1"), is(equalTo("13")));
//        Assert.assertThat(output.get(0).get("street2"), is(equalTo("Wikinews")));
//        Assert.assertThat(output.get(0).get("zip_code"), is(equalTo("English")));
//        Assert.assertThat(output.get(0).get("city"), is(equalTo("Spanish football: Sevilla signs Aleix Vidal from FC Barcelona")));
//        Assert.assertThat(output.get(0).get("country"), is(equalTo("12331")));
//        Assert.assertThat(output.get(0).get("created_at"), is(equalTo("12331")));
//        Assert.assertThat(output.get(0).get("updated_at"), is(equalTo("12331")));
//    }
//
//    @Test
//    public void test_parse_header_return_empty_list() throws Exception {
//
//        List<String> input = new ArrayList<>();
//
//        input.add("number,customer_id,street1,street2,zip_code,city,country,created_at,updated_at");
//
//        List<TableRow> output = fnTester.processBundle(input);
//
//        Assert.assertThat(output, is(empty()));
//    }
//}

public class TemplatePipelineTest {

        // Our static input data, which will make up the initial PCollection.
        static final String[] WORDS_ARRAY = new String[] {
                "hi", "there", "hi", "hi", "sue", "bob",
                "hi", "sue", "", "", "ZOW", "bob", ""};

        static final List<String> WORDS = Arrays.asList(WORDS_ARRAY);

        public void testCount() {
            // Create a test pipeline.
            Pipeline p = TestPipeline.create();

            // Create an input PCollection.
            PCollection<String> input = p.apply(Create.of(WORDS));

            // Apply the Count transform under test.
            PCollection<KV<String, Long>> output =
                    input.apply(Count.<String>perElement());

            // Assert on the results.
            PAssert.that(output)
                    .containsInAnyOrder(
                            KV.of("hi", 4L),
                            KV.of("there", 1L),
                            KV.of("sue", 2L),
                            KV.of("bob", 2L),
                            KV.of("", 3L),
                            KV.of("ZOW", 1L));

            // Run the pipeline.
            p.run();
        }
    }


