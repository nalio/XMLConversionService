package com.progress.codeshare.esbservice.xmlConversion;

import java.io.ByteArrayInputStream;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.Writer;
import java.util.Iterator;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathFactory;

import net.sf.saxon.om.ListIterator;
import net.sf.saxon.om.LookaheadIterator;
import net.sf.saxon.om.NamespaceConstant;
import net.sf.saxon.om.NodeInfo;

import org.xml.sax.InputSource;

import com.ddtek.xmlconverter.Converter;
import com.ddtek.xmlconverter.ConverterFactory;
import com.sonicsw.xq.XQConstants;
import com.sonicsw.xq.XQEnvelope;
import com.sonicsw.xq.XQInitContext;
import com.sonicsw.xq.XQMessage;
import com.sonicsw.xq.XQMessageFactory;
import com.sonicsw.xq.XQParameters;
import com.sonicsw.xq.XQPart;
import com.sonicsw.xq.XQService;
import com.sonicsw.xq.XQServiceContext;
import com.sonicsw.xq.XQServiceException;
import com.sonicsw.xq.service.sj.MessageUtils;

public final class XMLConversionService implements XQService {
	private static final ConverterFactory CONVERTER_FACTORY = new ConverterFactory();

	private static final String DIRECTION_FROM = "From";

	private static final String DIRECTION_TO = "To";

	private static final String OUTPUT_LEVEL_MESSAGE = "Message";

	private static final String OUTPUT_LEVEL_MESSAGE_PART = "Message Part";

	private static final String PARAM_NAME_DIRECTION = "direction";

	private static final String PARAM_NAME_KEEP_ORIGINAL_PART = "keepOriginalPart";

	private static final String PARAM_NAME_MESSAGE_PART = "messagePart";

	private static final String PARAM_NAME_OUTPUT_LEVEL = "outputLevel";

	private static final String PARAM_NAME_URI = "uri";

	private static final String PARAM_NAME_XPATH_EXPRESSION = "xpathExpression";

	public void destroy() {
	}

	public void init(XQInitContext initCtx) {
	}

	public void service(final XQServiceContext servCtx)
			throws XQServiceException {

		try {
			final XQParameters params = servCtx.getParameters();

			final String direction = params.getParameter(PARAM_NAME_DIRECTION,
					XQConstants.PARAM_STRING);

			final XQMessageFactory msgFactory = servCtx.getMessageFactory();

			final int messagePart = params.getIntParameter(
					PARAM_NAME_MESSAGE_PART, XQConstants.PARAM_STRING);

			final boolean keepOriginalPart = params.getBooleanParameter(
					PARAM_NAME_KEEP_ORIGINAL_PART, XQConstants.PARAM_STRING);

			final String uri = params.getParameter(PARAM_NAME_URI,
					XQConstants.PARAM_STRING);

			if (direction.equals(DIRECTION_FROM)) {
				/* Set the converter up */
				final Converter converter = CONVERTER_FACTORY
						.newConvertFromXML(uri);

				final String xpathExpression = params.getParameter(
						PARAM_NAME_XPATH_EXPRESSION, XQConstants.PARAM_STRING);

				if (xpathExpression != null) {
					/* Set the XPath factory up */
					final XPathFactory xpathFactory = XPathFactory
							.newInstance(NamespaceConstant.OBJECT_MODEL_SAXON);

					final XPath xpath = xpathFactory.newXPath();

					final String outputLevel = params.getParameter(
							PARAM_NAME_OUTPUT_LEVEL, XQConstants.PARAM_STRING);

					while (servCtx.hasNextIncoming()) {
						final XQEnvelope env = servCtx.getNextIncoming();

						final XQMessage origMsg = env.getMessage();

						final Iterator addressIterator = env.getAddresses();

						for (int i = 0; i < origMsg.getPartCount(); i++) {

							/* Decide whether to process the part or not */
							if ((messagePart == i)
									|| (messagePart == XQConstants.ALL_PARTS)) {
								final XQPart origPart = origMsg.getPart(i);

								final List resultList = (List) xpath
										.evaluate(
												xpathExpression,
												new InputSource(
														new StringReader(
																(String) origPart
																		.getContent())),
												XPathConstants.NODESET);

								if (resultList != null) {
									final LookaheadIterator resultIterator = new ListIterator(
											resultList);

									if (outputLevel
											.equals(OUTPUT_LEVEL_MESSAGE)) {

										/* Create new messages from the results */
										while (resultIterator.hasNext()) {
											final XQMessage newMsg = msgFactory
													.createMessage();

											/*
											 * Copy all headers from the
											 * original message to the new
											 * message
											 */
											MessageUtils.copyAllHeaders(
													origMsg, newMsg);

											if (keepOriginalPart) {
												origPart
														.setContentId("original_part_"
																+ i);

												newMsg.addPart(origPart);
											}

											final XQPart newPart = newMsg
													.createPart();

											newPart
													.setContentId("XMLConvertedMessage-"
															+ i
															+ "_"
															+ resultIterator
																	.position());

											final Writer out = new StringWriter();

											/* Convert the original part */
											converter.convert(
													(NodeInfo) resultIterator
															.next(),
													new StreamResult(out));

											newPart
													.setContent(
															out.toString(),
															XQConstants.CONTENT_TYPE_TEXT);

											newMsg.addPart(newPart);

											env.setMessage(newMsg);

											if (addressIterator.hasNext())
												servCtx.addOutgoing(env);

										}

									} else if (outputLevel
											.equals(OUTPUT_LEVEL_MESSAGE_PART)) {
										final XQMessage newMsg = msgFactory
												.createMessage();

										/*
										 * Copy all headers from the original
										 * message to the new message
										 */
										MessageUtils.copyAllHeaders(origMsg,
												newMsg);

										if (keepOriginalPart) {
											origPart
													.setContentId("original_part_"
															+ i);

											newMsg.addPart(origPart);
										}

										/* Create new parts from the results */
										while (resultIterator.hasNext()) {
											final XQPart newPart = newMsg
													.createPart();

											newPart
													.setContentId("XMLConvertedMessage-"
															+ i
															+ "_"
															+ resultIterator
																	.position());

											final Writer out = new StringWriter();

											/* Convert the original part */
											converter.convert(
													(NodeInfo) resultIterator
															.next(),
													new StreamResult(out));

											newPart
													.setContent(
															out.toString(),
															XQConstants.CONTENT_TYPE_TEXT);

											newMsg.addPart(newPart);
										}

										env.setMessage(newMsg);

										if (addressIterator.hasNext())
											servCtx.addOutgoing(env);

									}

								}

							}

							/* Break when done */
							if (messagePart == i)
								break;

						}

					}

				} else {
					/* Set the document builder up */
					final DocumentBuilderFactory builderFactory = DocumentBuilderFactory
							.newInstance();

					builderFactory.setNamespaceAware(true);

					final DocumentBuilder builder = builderFactory
							.newDocumentBuilder();

					while (servCtx.hasNextIncoming()) {
						final XQEnvelope env = servCtx.getNextIncoming();

						final XQMessage origMsg = env.getMessage();

						final Iterator addressIterator = env.getAddresses();

						for (int i = 0; i < origMsg.getPartCount(); i++) {

							/* Decide whether to process the part or not */
							if ((messagePart == i)
									|| (messagePart == XQConstants.ALL_PARTS)) {
								final XQMessage newMsg = msgFactory
										.createMessage();

								/*
								 * Copy all headers from the original message to
								 * the new message
								 */
								MessageUtils.copyAllHeaders(origMsg, newMsg);

								final XQPart origPart = origMsg.getPart(i);

								/*
								 * Decide whether to keep the original part or
								 * not
								 */
								if (keepOriginalPart) {
									origPart.setContentId("original_part_" + i);

									newMsg.addPart(origPart);
								}

								final XQPart newPart = newMsg.createPart();

								newPart
										.setContentId("XMLConvertedMessage-"
												+ i);

								final Writer out = new StringWriter();

								/* Convert the original part */
								converter.convert(
										new DOMSource(builder
												.parse((String) origPart
														.getContent())),
										new StreamResult(out));

								newPart.setContent(out.toString(),
										XQConstants.CONTENT_TYPE_TEXT);

								newMsg.addPart(newPart);

								env.setMessage(newMsg);

								if (addressIterator.hasNext())
									servCtx.addOutgoing(env);

							}

							/* Break when done */
							if (messagePart == i)
								break;

						}

					}

				}

			} else if (direction.equals(DIRECTION_TO)) {
				final Converter converter = CONVERTER_FACTORY
						.newConvertToXML(uri);

				while (servCtx.hasNextIncoming()) {
					final XQEnvelope env = servCtx.getNextIncoming();

					final XQMessage origMsg = env.getMessage();

					final Iterator addressIterator = env.getAddresses();

					for (int i = 0; i < origMsg.getPartCount(); i++) {

						/* Decide whether to process the part or not */
						if ((messagePart == i)
								|| (messagePart == XQConstants.ALL_PARTS)) {
							final XQMessage newMsg = msgFactory.createMessage();

							/*
							 * Copy all headers from the original message to the
							 * new message
							 */
							MessageUtils.copyAllHeaders(origMsg, newMsg);

							final XQPart origPart = origMsg.getPart(i);

							/* Decide whether to keep the original part or not */
							if (keepOriginalPart) {
								origPart.setContentId("original_part_" + i);

								newMsg.addPart(origPart);
							}

							final XQPart newPart = newMsg.createPart();

							newPart.setContentId("XMLConvertedMessage-" + i);

							final Writer out = new StringWriter();

							/* Convert the original part */
							converter.convert(new StreamSource(
									new ByteArrayInputStream(((String) origPart
											.getContent()).getBytes())),
									new StreamResult(out));

							newPart.setContent(out.toString(),
									XQConstants.CONTENT_TYPE_XML);

							newMsg.addPart(newPart);

							env.setMessage(newMsg);

							if (addressIterator.hasNext())
								servCtx.addOutgoing(env);

						}

						/* Break when done */
						if (messagePart == i)
							break;

					}

				}

			}

		} catch (final Exception e) {
			throw new XQServiceException(e);
		}

	}

}